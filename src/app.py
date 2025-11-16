import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import re
from io import StringIO

def extract_number(x):
    """
    Extrae el primer número (entero o decimal) encontrado en un string.
    Si no hay número, devuelve None.
    """
    match = re.search(r"\d+\.\d+|\d+", str(x))
    return float(match.group()) if match else None


def scrape_spotify_table():
    """
    Descarga la página de Wikipedia de Spotify Streaming Records,
    identifica la primera tabla de tipo 'wikitable' y la convierte en DataFrame.
    """
    url = "https://en.wikipedia.org/wiki/List_of_Spotify_streaming_records"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/120.0.0.0 Safari/537.36"
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise Exception(f"Error al descargar la página. Código: {response.status_code}")

    soup = BeautifulSoup(response.text, "html.parser")

    tables = soup.find_all("table", class_="wikitable")

    if len(tables) == 0:
        raise Exception("No se encontraron tablas 'wikitable' en la página.")

    # Leer la primera tabla
    df = pd.read_html(StringIO(str(tables[0])))[0]

    return df


def clean_dataframe(df):
    """
    Renombra columnas, extrae valores numéricos válidos para Streams,
    convierte tipos y elimina filas sin datos útiles.
    """

    # Renombrar columnas según la tabla real
    df.columns = [
        "Rank",
        "Song",
        "Artist(s)",
        "Streams (billions)",
        "Release date",
        "Ref"
    ]

    # Extraer solo números de la columna Streams (por si hay texto como "As of 2025")
    df["Streams (billions)"] = df["Streams (billions)"].apply(extract_number)

    # Convertir Rank a número
    df["Rank"] = pd.to_numeric(df["Rank"], errors="coerce")

    # Eliminar filas que no tienen streams válidos
    df = df.dropna(subset=["Streams (billions)"])

    return df


def save_to_sqlite(df):
    """
    Guarda un DataFrame en una base de datos SQLite.
    """

    conn = sqlite3.connect("spotify_streaming_records.db")

    df.to_sql("most_streamed_songs", conn, if_exists="replace", index=False)

    conn.commit()
    conn.close()


def main():
    print("Descargando tabla desde Wikipedia...")
    df = scrape_spotify_table()

    print("Limpiando datos...")
    df = clean_dataframe(df)

    print("Guardando datos en SQLite...")
    save_to_sqlite(df)

    print("\nProceso completado exitosamente.")
    print("Base de datos generada: spotify_streaming_records.db")
    print("Tabla guardada: most_streamed_songs")


if __name__ == "__main__":
    main()

