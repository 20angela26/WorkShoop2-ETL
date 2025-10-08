from __future__ import annotations

from datetime import datetime
import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
import logging
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

load_dotenv()




SQLITE_CONN_ID='sqlite_grammys'
MYSQL_CONN_ID='mysql_dw'
SPOTIFY_CSV_PATH='data/spotify_clean_final.csv'
GRAMMY_TABLE='grammy_records'
BATCH_SIZE=1000


def extract_spotify_data_from_csv(path: str) -> pd.DataFrame:
    """Lee los datos de Spotify desde un archivo CSV."""
    print(f"Leyendo datos de Spotify desde: {path}")
    df = pd.read_csv(path)
    print("Vista previa de los datos de Spotify:")
    print(df.head())
    return df


def extract_grammy_data_from_sqlite(sqlite_conn, table_name: str) -> pd.DataFrame:
    """Lee los datos de los Grammys desde una conexión a SQLite."""
    print(f"Leyendo la tabla '{table_name}' de la base de datos de Grammys...")
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, sqlite_conn)
    print("Vista previa de los datos de Grammys:")
    print(df.head())
    return df


def merge_spotify_and_grammys(df_spotify: pd.DataFrame, df_grammy: pd.DataFrame) -> pd.DataFrame:
    """Normaliza y une los DataFrames de Spotify y Grammys."""
    print("Iniciando la transformación y unión de datos...")

   
    df_grammy = df_grammy.rename(columns={"nominee": "track_name", "artist": "artists"})

    
    if "artists" in df_spotify.columns and df_spotify["artists"].apply(lambda x: isinstance(x, list)).any():
        df_spotify["artists"] = df_spotify["artists"].apply(
            lambda x: ", ".join(x) if isinstance(x, list) else (x if pd.notna(x) else "")
        )

    for df in (df_spotify, df_grammy):
        for col in ["track_name", "artists"]:
            if col in df.columns:
                df[col] = df[col].fillna("").astype(str).str.strip().str.lower()
            else:
                df[col] = ""

    print("Columnas de unión normalizadas.")
    print("Ejemplo Spotify:", df_spotify[["track_name", "artists"]].head(2))
    print("Ejemplo Grammy:", df_grammy[["track_name", "artists"]].head(2))

    df_merged = pd.merge(
        df_spotify,
        df_grammy,
        on=["track_name", "artists"],
        how="outer",
        indicator=True,
    )

    print("Merge completado.")
    print("Dimensiones del DataFrame unido:", df_merged.shape)
    print("Distribución de '_merge':")
    print(df_merged["_merge"].value_counts())

    return df_merged


def save_dataframe_to_parquet(df: pd.DataFrame, run_ts: str, base_dir: str = "/tmp") -> str:
    """Guarda el DataFrame a Parquet y devuelve la ruta."""
    path = f"{base_dir}/merged_{run_ts}.parquet"
    df.to_parquet(path, index=False)
    print(f"Archivo Parquet guardado en: {path}")
    return path



@task
def extract_spotify() -> list[dict]:
    df = extract_spotify_data_from_csv(SPOTIFY_CSV_PATH)
    return df.to_dict(orient="records")


@task
def extract_grammys() -> list[dict]:
    hook = SqliteHook(sqlite_conn_id=SQLITE_CONN_ID)
    with hook.get_conn() as conn:
        df = extract_grammy_data_from_sqlite(conn, GRAMMY_TABLE)
    return df.to_dict(orient="records")


@task
def merge_and_transform(spotify_records: list[dict], grammy_records: list[dict]) -> dict:
    """Une, transforma y guarda los datos en Parquet (ruta y filas como metadata)."""
    ctx = get_current_context()
    run_ts = ctx["ts_nodash"]

    df_spotify = pd.DataFrame(spotify_records)
    df_grammy = pd.DataFrame(grammy_records)

    df_merged = merge_spotify_and_grammys(df_spotify, df_grammy)
    output_path = save_dataframe_to_parquet(df_merged, run_ts)
    return {"path": output_path, "rows": int(len(df_merged))}


@task
def load_to_warehouse(metadata: dict) -> dict:
    """
    Carga datos desde un archivo Parquet a un Data Warehouse en MySQL.
    
    Args:
        metadata (dict): Contiene la ruta al archivo Parquet ('path').
    
    Returns:
        dict: Resumen de filas insertadas y procesadas.
    """
   
    logger = logging.getLogger("airflow.task") 
    
    try:
        
        path = metadata["path"]
        logger.info(f"Leyendo archivo Parquet desde: {path}")
        df_merged = pd.read_parquet(path)
        logger.info(f"Datos leídos. Total de filas: {len(df_merged)}")

    
        mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
        url = mysql_hook.get_uri()
        engine = create_engine(url, pool_pre_ping=True, pool_size=5, max_overflow=10, pool_timeout=30)
        
        inserted_spotify = 0
        inserted_grammy = 0

        def insert_dim(conn, table: str, columns: list[str], values: list, id_col: str = "id") -> int:
            """Inserta una fila en una tabla de dimensión y devuelve el ID. Si existe, lo selecciona."""
            payload = {c: (None if pd.isna(v) else v) for c, v in zip(columns, values)}
            try:
                ins = text(
                    f"INSERT INTO {table} ({', '.join(columns)}) "
                    f"VALUES ({', '.join(':'+c for c in columns)})"
                )
                conn.execute(ins, payload)
                last_id = conn.execute(text("SELECT LAST_INSERT_ID()")).scalar()
                return last_id
            except IntegrityError:
                sel = text(
                    f"SELECT {id_col} FROM {table} WHERE "
                    + " AND ".join(f"{c} = :{c}" for c in columns)
                    + " LIMIT 1"
                )
                return conn.execute(sel, payload).scalar()

        def insert_batch(conn, table: str, columns: list[str], rows: list[dict]):
            """Inserta un lote de filas en la tabla especificada usando executemany."""
            
            
            param_list = []
            for col in columns:
                if col == '`key`':
                    param_list.append(':track_key') 
                else:
                    param_list.append(':' + col.replace("`", "")) 

            ins = text(
                f"INSERT INTO {table} ({', '.join(columns)}) "
                f"VALUES ({', '.join(param_list)})"
            )
            conn.execute(ins, rows)

        with engine.begin() as conn:
           
            for start in range(0, len(df_merged), BATCH_SIZE):
                batch = df_merged[start:start + BATCH_SIZE].to_dict(orient="records")
                logger.info(f"Procesando lote {start // BATCH_SIZE + 1} ({start} a {start + len(batch)} filas)")

                spotify_rows = []
                grammy_rows = []

                for row in batch:
                    
                    song_name = row.get("track_name") or row.get("nominee")
                    song_id = (
                        insert_dim(conn, "Dim_Song", ["song_name"], [song_name], "song_id")
                        if (song_name and pd.notna(song_name))
                        else None
                    )

                    artist_name = row.get("artists") or row.get("artist")
                    artist_id = (
                        insert_dim(conn, "Dim_Artist", ["artist_name"], [artist_name], "artist_id")
                        if (artist_name and pd.notna(artist_name))
                        else None
                    )

                    album_name = row.get("album_name")
                    album_id = (
                        insert_dim(conn, "Dim_Album", ["album_name"], [album_name], "album_id")
                        if (album_name and pd.notna(album_name))
                        else None
                    )

                    genero = row.get("genero")
                    subgenero = row.get("subgenero")
                    genre_id = (
                        insert_dim(conn, "Dim_Genre", ["genero", "subgenero"], [genero, subgenero], "genre_id")
                        if (pd.notna(genero) and pd.notna(subgenero) and str(genero) != "" and str(subgenero) != "")
                        else None
                    )

                    category = row.get("category")
                    category_id = (
                        insert_dim(conn, "Dim_Category", ["category"], [category], "category_id")
                        if (pd.notna(category) and category != "")
                        else None
                    )

                    year = row.get("year")
                    title = row.get("title")
                    published_at = row.get("published_at")
                    updated_at = row.get("updated_at")

                    event_id = None
                    if pd.notna(year) and pd.notna(title):
                        event_cols = ["year", "title"]
                        event_vals = [year, title]
                        if pd.notna(published_at):
                            event_cols.append("published_at")
                            event_vals.append(published_at)
                        if pd.notna(updated_at):
                            event_cols.append("updated_at")
                            event_vals.append(updated_at)
                        event_id = insert_dim(conn, "Dim_Event", event_cols, event_vals, "event_id")

                  
                    if pd.notna(row.get("track_id")) and song_id and artist_id and genre_id:
                        spotify_columns = [
                            "song_id", "artist_id", "album_id", "genre_id", "track_id",
                            "unnamed_0", "popularity", "duration_ms", "explicit", "danceability",
                            "energy", "`key`", "loudness", "mode", "speechiness", "acousticness",
                            "instrumentalness", "liveness", "valence", "tempo", "time_signature",
                        ]
                        
                        spotify_values = {
                            "song_id": song_id,
                            "artist_id": artist_id,
                            "album_id": album_id,
                            "genre_id": genre_id,
                            "track_id": row.get("track_id"),
                            "unnamed_0": row.get("Unnamed: 0") or row.get("unnamed_0"),
                            "popularity": row.get("popularity"),
                            "duration_ms": row.get("duration_ms"),
                            "explicit": row.get("explicit"),
                            "danceability": row.get("danceability"),
                            "energy": row.get("energy"),
                            "track_key": row.get("key"),  
                            "loudness": row.get("loudness"),
                            "mode": row.get("mode"),
                            "speechiness": row.get("speechiness"),
                            "acousticness": row.get("acousticness"),
                            "instrumentalness": row.get("instrumentalness"),
                            "liveness": row.get("liveness"),
                            "valence": row.get("valence"),
                            "tempo": row.get("tempo"),
                            "time_signature": row.get("time_signature"),
                        }
                        spotify_rows.append(spotify_values)
                        inserted_spotify += 1

                    if pd.notna(row.get("year")) and song_id and artist_id and category_id and event_id:
                        grammy_columns = ["song_id", "artist_id", "category_id", "event_id", "workers", "img", "winner"]
                        grammy_values = {
                            "song_id": song_id,
                            "artist_id": artist_id,
                            "category_id": category_id,
                            "event_id": event_id,
                            "workers": row.get("workers"),
                            "img": row.get("img"),
                            "winner": row.get("winner"),
                        }
                        grammy_rows.append(grammy_values)
                        inserted_grammy += 1

             
                if spotify_rows:
                   
                    insert_batch(conn, "Fact_Spotify_Tracks", spotify_columns, spotify_rows)
                if grammy_rows:
                   
                    insert_batch(conn, "Fact_Grammy_Awards", grammy_columns, grammy_rows)

                logger.info(f"Lote {start // BATCH_SIZE + 1} confirmado: {len(batch)} filas procesadas")

            logger.info("Carga de datos al Data Warehouse completada.")

            result = {
                "inserted_spotify": inserted_spotify,
                "inserted_grammy": inserted_grammy,
                "rows": len(df_merged),
                "path": path,
            }
            logger.info(f"Resumen: {result}")
            return result

    except Exception as e:
        logger.error(f"Error durante la carga: {e}")
        raise e


@dag(
    dag_id="etl_spotify_grammys",
    start_date=datetime(2025, 9, 1),
    schedule=None,
    catchup=False,
    tags=["spotify", "grammys", "etl"],
    doc_md="""
    ### DAG de ETL para Spotify y Grammys
    Extrae datos de Spotify (CSV) y Grammys (SQLite), los unifica y
    carga a un Data Warehouse (MySQL) con modelo dimensional.
    """
)
def etl_spotify_grammys():
    spotify_records = extract_spotify()
    grammy_records = extract_grammys()
    metadata = merge_and_transform(spotify_records, grammy_records)
    load_to_warehouse(metadata)

dag = etl_spotify_grammys()