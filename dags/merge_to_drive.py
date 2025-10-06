from __future__ import annotations
from datetime import datetime
import os
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import os
from dotenv import load_dotenv

load_dotenv()


# Importa funciones desde tu merge.py
from merge_to_dw import (
    extract_spotify_data_from_csv,
    extract_grammy_data_from_sqlite,
    merge_spotify_and_grammys,
    SPOTIFY_CSV_PATH,
    GRAMMY_TABLE,
)


SYNC_DIR = os.getenv("SYNC_DIR") 
SQLITE_CONN_ID = os.getenv("SQLITE_CONN_ID")


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def save_dataframe_to_csv(df: pd.DataFrame, run_ts: str, base_dir: str) -> str:
    ensure_dir(base_dir)
    path = os.path.join(base_dir, f"merged_{run_ts}.csv")
    df.to_csv(path, index=False, encoding="utf-8")
    print(f"Archivo CSV guardado en: {path}")
    return path

@dag(
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["merge", "csv", "drive"],
)
def merge_spotify_grammys_to_drive():
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
    def merge_and_save(spotify_records: list[dict], grammy_records: list[dict]) -> dict:
        ctx = get_current_context()
        run_ts = ctx["ts_nodash"]

        df_spotify = pd.DataFrame(spotify_records)
        df_grammy = pd.DataFrame(grammy_records)

        df_merged = merge_spotify_and_grammys(df_spotify, df_grammy)
        output_path = save_dataframe_to_csv(df_merged, run_ts, base_dir=SYNC_DIR)
        return {"path": output_path, "rows": int(len(df_merged))}

    
    s = extract_spotify()
    g = extract_grammys()
    merge_and_save(s, g)

dag_obj = merge_spotify_grammys_to_drive()
