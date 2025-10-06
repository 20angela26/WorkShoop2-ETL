import sqlite3
import pandas as pd


ruta_csv = 'data/the_grammy_awards.csv'
df = pd.read_csv(ruta_csv)
conn = sqlite3.connect('grammys.db')
cursor = conn.cursor()


cursor.execute('''
CREATE TABLE IF NOT EXISTS grammy_records (
    year INTEGER,
    title TEXT,
    published_at TEXT,
    updated_at TEXT,
    category TEXT,
    nominee TEXT,
    artist TEXT,
    workers TEXT,
    img TEXT,
    winner BOOLEAN
)
''')


for _, row in df.iterrows():
    cursor.execute('''
    INSERT INTO grammy_records (year, title, published_at, updated_at, category, nominee, artist, workers, img, winner)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        row['year'],
        row['title'],
        row['published_at'],
        row['updated_at'],
        row['category'],
        row['nominee'],
        row['artist'],
        row['workers'],
        row['img'],
        row['winner']
    ))


conn.commit()
conn.close()

print("Datos cargados desde CSV y guardados en la base de datos.")