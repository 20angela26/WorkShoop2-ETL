from __future__ import annotations
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd
from pathlib import Path


CSV_FILE_IN  = "data/spotify_dataset.csv"
CSV_FILE_OUT = "data/spotify_clean_final.csv"


mapa_generos = {
  
    'rock': ('Rock','Rock'), 'alt-rock': ('Rock','Alternativo'), 'punk-rock': ('Rock','Punk Rock'),
    'punk': ('Rock','Punk'), 'hard-rock': ('Rock','Hard Rock'), 'grunge': ('Rock','Grunge'),
    'psych-rock': ('Rock','Psychedelic Rock'), 'rock-n-roll': ('Rock','Rock & Roll'),
    'rockabilly': ('Rock','Rockabilly'), 'emo': ('Rock','Emo'), 'goth': ('Rock','Goth'),
    'garage': ('Rock','Garage Rock'), 'british': ('Rock','British Rock'), 'guitar': ('Rock','Guitar Rock'),
    'industrial': ('Rock','Industrial'), 'ska': ('Rock','Ska'), 'j-rock': ('Rock','J-Rock'),
    'metal': ('Metal','Metal'), 'heavy-metal': ('Metal','Heavy Metal'), 'black-metal': ('Metal','Black Metal'),
    'death-metal': ('Metal','Death Metal'), 'metalcore': ('Metal','Metalcore'), 'hardcore': ('Metal','Hardcore'),
    'grindcore': ('Metal','Grindcore'),
    'pop': ('Pop','Pop'), 'indie-pop': ('Pop','Indie Pop'), 'power-pop': ('Pop','Power Pop'),
    'synth-pop': ('Pop','Synth Pop'), 'pop-film': ('Pop','Pop Film'), 'alternative': ('Pop','Alternative Pop'),
    'indie': ('Pop','Indie'), 'anime': ('Pop','Anime Pop'), 'k-pop': ('Pop','K-Pop'),
    'j-pop': ('Pop','J-Pop'), 'cantopop': ('Pop','CantoPop'), 'mandopop': ('Pop','MandoPop'),
    'j-dance': ('Pop','J-Dance'), 'j-idol': ('Pop','J-Idol'),
    'electronic': ('Electrónica','Electronic'), 'edm': ('Electrónica','EDM'), 'electro': ('Electrónica','Electro'),
    'house': ('Electrónica','House'), 'deep-house': ('Electrónica','Deep House'),
    'chicago-house': ('Electrónica','Chicago House'), 'progressive-house': ('Electrónica','Progressive House'),
    'minimal-techno': ('Electrónica','Minimal Techno'), 'techno': ('Electrónica','Techno'),
    'detroit-techno': ('Electrónica','Detroit Techno'), 'trance': ('Electrónica','Trance'),
    'idm': ('Electrónica','IDM'), 'dubstep': ('Electrónica','Dubstep'), 'dub': ('Electrónica','Dub'),
    'drum-and-bass': ('Electrónica','Drum & Bass'), 'breakbeat': ('Electrónica','Breakbeat'),
    'trip-hop': ('Electrónica','Trip-Hop'), 'club': ('Electrónica','Club'), 'dance': ('Electrónica','Dance'),
    'dancehall': ('Electrónica','Dancehall'), 'disco': ('Electrónica','Disco'), 'hardstyle': ('Electrónica','Hardstyle'),
    'hip-hop': ('Hip-Hop','Hip-Hop'), 'r-n-b': ('Hip-Hop','R&B'), 'soul': ('Hip-Hop','Soul'),
    'funk': ('Hip-Hop','Funk'), 'gospel': ('Hip-Hop','Gospel'), 'afrobeat': ('Hip-Hop','Afrobeat'),
    'classical': ('Clásica','Classical'), 'opera': ('Clásica','Opera'), 'piano': ('Clásica','Piano'),
    'jazz': ('Clásica','Jazz'), 'blues': ('Clásica','Blues'), 'bluegrass': ('Clásica','Bluegrass'),
    'latin': ('Latina','Latin'), 'latino': ('Latina','Latino'), 'salsa': ('Latina','Salsa'),
    'samba': ('Latina','Samba'), 'pagode': ('Latina','Pagode'), 'sertanejo': ('Latina','Sertanejo'),
    'brazil': ('Latina','Brazil'), 'mpb': ('Latina','MPB'), 'forro': ('Latina','Forró'),
    'reggaeton': ('Latina','Reggaeton'), 'reggae': ('Latina','Reggae'), 'tango': ('Latina','Tango'),
    'world-music': ('World Music','World Music'), 'turkish': ('World Music','Turkish'),
    'indian': ('World Music','Indian'), 'iranian': ('World Music','Iranian'), 'malay': ('World Music','Malay'),
    'french': ('World Music','French'), 'german': ('World Music','German'),
    'swedish': ('World Music','Swedish'), 'spanish': ('World Music','Spanish'),
    'folk': ('Country','Folk'), 'country': ('Country','Country'), 'honky-tonk': ('Country','Honky-Tonk'),
    'singer-songwriter': ('Country','Singer-Songwriter'), 'songwriter': ('Country','Songwriter'),
    'acoustic': ('Country','Acoustic'),
    'children': ('Infantil','Children'), 'kids': ('Infantil','Kids'), 'disney': ('Infantil','Disney'),
    'comedy': ('Infantil','Comedy'), 'show-tunes': ('Infantil','Show Tunes'), 'new-age': ('Infantil','New Age'),
    'chill': ('mood','Chill'), 'study': ('mood','Study'), 'sleep': ('mood','Sleep'),
    'romance': ('mood','Romance'), 'happy': ('mood','Happy'), 'party': ('mood','Party'),
    'ambient': ('mood','Ambient'), 'sad': ('mood','Sad'), 'groove': ('mood','Groove'),
}

def _mode_or_first(s: pd.Series):
    m = s.mode(dropna=True)
    return m.iloc[0] if len(m) > 0 else (s.dropna().iloc[0] if s.dropna().size else None)

@dag(
    dag_id="spotify_clean",
    start_date=days_ago(1),
    schedule="0 2 * * SUN",
    catchup=False,
    tags=["spotify","cleaning","minimal"]
)
def spotify_clean():

    @task
    def transform_and_write():
       
        df = pd.read_csv(CSV_FILE_IN)

  
        required_cols = {"track_id", "track_genre", "popularity"}
        missing = required_cols - set(df.columns)
        if missing:
            raise ValueError(f"Faltan columnas requeridas: {missing}")


        genero_map = {k: v[0] for k, v in mapa_generos.items()}
        sub_map    = {k: v[1] for k, v in mapa_generos.items()}

        df['track_genre'] = df['track_genre'].astype(str)
        df['genero']    = df['track_genre'].map(genero_map).fillna('Otro')
        df['subgenero'] = df['track_genre'].map(sub_map).fillna(df['track_genre'])

      
        df = df.drop(columns=['track_genre'])

     
        def agg_col(s: pd.Series):
            return _mode_or_first(s) if s.name == 'popularity' else s.iloc[0]

        df = df.groupby('track_id', as_index=False).agg(agg_col)

        
        df = df.drop_duplicates()

        Path(CSV_FILE_OUT).parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(CSV_FILE_OUT, index=False)
        print(f"[OK] Escrito {CSV_FILE_OUT} con {df.shape[0]} filas.")

    transform_and_write()

dag_obj = spotify_clean()