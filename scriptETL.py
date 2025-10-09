import pandas as pd
from pandas import read_csv
import os
import json #leer credenciales
from sqlalchemy import create_engine #pasar a mysql

# ================================================================
# ETAPA 1 y 2 - EXTRACT and TRANSFORM
# ================================================================

# === CONFIGURACIÓN ===
archivos_netflix = [
    "archivosCSV\combined_data_1.txt",
    "archivosCSV\combined_data_2.txt",
    "archivosCSV\combined_data_3.txt",
    "archivosCSV\combined_data_4.txt"
]
archivo_titulos = "archivosCSV\movie_titles.csv"
salida_csv = "netflix.csv"

chunk_size = 1_000_000  # cantidad de registros a acumular antes de escribir al CSV para no quedarme sin RAM

# Lee el archivo txt
def procesar_archivo_netflix(ruta, writer_mode='w'):
    data = []
    movie_id = None
    line_count = 0

    with open(ruta, "r", encoding="latin-1") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue
            if linea.endswith(":"):
                movie_id = int(linea[:-1])
            else:
                customer_id, rating, date = linea.split(",")
                data.append((movie_id, int(customer_id), int(rating), date))

                if len(data) >= chunk_size:
                    df_chunk = pd.DataFrame(data, columns=["movie_id", "customer_id", "rating", "date"])
                    df_chunk.to_csv(salida_csv, mode=writer_mode, header=(writer_mode == 'w'), index=False)
                    data = []
                    writer_mode = 'a'

            line_count += 1
            if line_count % 1_000_000 == 0:
                print(f"Procesadas {line_count:,} líneas de {ruta}")

    if data:
        df_chunk = pd.DataFrame(data, columns=["movie_id", "customer_id", "rating", "date"])
        df_chunk.to_csv(salida_csv, mode=writer_mode, header=(writer_mode == 'w'), index=False)

    print(f" Archivo {ruta} procesado completamente.")

if os.path.exists(salida_csv):
    os.remove(salida_csv)

for i, archivo in enumerate(archivos_netflix):
    modo = 'w' if i == 0 else 'a'
    procesar_archivo_netflix(archivo, writer_mode=modo)

print("✅ Todos los archivos combinados en:", salida_csv)

print("🔄 Enlazando títulos de películas...")

ratings = pd.read_csv(salida_csv)
def leer_titulos_con_comas(archivo_titulos):
    data = []
    with open(archivo_titulos, encoding="latin-1") as f:
        for line in f:
            partes = line.strip().split(",", 2)
            if len(partes) == 3:
                movie_id, year, movie_name = partes
                data.append([int(movie_id), year, movie_name])
            else:
                continue
    return pd.DataFrame(data, columns=["movie_id", "year", "movie_name"])

movies = leer_titulos_con_comas(archivo_titulos)
merged = pd.merge(ratings, movies, on="movie_id", how="left")
merged.to_csv("netflix_full_with_titles.csv", index=False)
print("🎉 Dataset final guardado como netflix_full_with_titles.csv")

# ================================
# ETAPA 3 - LOAD
# ================================

# === Cargar credenciales desde archivo externo ===
with open("config_db.json") as f:
    config = json.load(f)

usuario = config["user"]
contraseña = config["password"]
host = config["host"]
base_datos = config["database"]

# === Crear conexión con SQLAlchemy ===
engine = create_engine(f"mysql+mysqlconnector://{usuario}:{contraseña}@{host}/{base_datos}")

# === Leer CSV y cargar a MySQL ===
archivo_csv = "netflix_full_with_titles.csv"
tabla = "ratings"
chunk_size = 100_000
i = 0

for chunk in pd.read_csv(archivo_csv, chunksize=chunk_size):
    # Opcional: convertir fechas
    if "date" in chunk.columns:
        chunk["date"] = pd.to_datetime(chunk["date"], errors="coerce")

    # Insertar en MySQL
    chunk.to_sql(name=tabla, con=engine, if_exists="append", index=False)
    i += len(chunk)
    print(f"✅ Insertadas {i:,} filas...")

print("🎉 Carga completa en MySQL.")
