import pandas as pd
import os
from datetime import datetime

# ================================================================
# ETAPA 1 y 2 - EXTRACT and TRANSFORM
# ================================================================

# === CONFIGURACIÓN ===
archivos_netflix = [
    "archivosCSV/combined_data_1.txt",
    "archivosCSV/combined_data_2.txt",
    "archivosCSV/combined_data_3.txt",
    "archivosCSV/combined_data_4.txt"
]
archivo_titulos = "archivosCSV/movie_titles.csv"
salida_csv_temp = "netflix_ratings_temp.csv"

# Directorios de salida para las dimensiones y hechos
OUTPUT_DIR = "output_dw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

chunk_size = 1_000_000
TOPE_FILAS = 1_000_000  # <= TOPE de registros para fact_rating

print("=" * 60)
print("INICIANDO PROCESO ETL - DATA WAREHOUSE NETFLIX")
print("=" * 60)

# ================================================================
# PASO 1: EXTRAER Y TRANSFORMAR RATINGS (con TOPE)
# ================================================================

def procesar_archivo_netflix(ruta, writer_mode='w', ratings_generados=0, TOPE_FILAS=TOPE_FILAS):
    """
    Procesa archivos de ratings de Netflix y escribe a 'salida_csv_temp'
    sin superar 'TOPE_FILAS'. Devuelve (ratings_generados_actualizados, llego_al_tope: bool)
    """
    data = []
    movie_id = None
    line_count = 0
    llego_al_tope = False

    remaining = TOPE_FILAS - ratings_generados
    if remaining <= 0:
        return ratings_generados, True

    with open(ruta, "r", encoding="latin-1") as f:
        for linea in f:
            linea = linea.strip()
            if not linea:
                continue

            if linea.endswith(":"):
                movie_id = int(linea[:-1])
            else:
                # registro de rating
                if remaining <= 0:
                    llego_al_tope = True
                    break

                customer_id, rating, date = linea.split(",")
                data.append((movie_id, int(customer_id), int(rating), date))

                # ¿tenemos que volcar un chunk?
                if len(data) >= chunk_size or len(data) >= remaining:
                    # recortar si el buffer supera el restante permitido
                    if len(data) > remaining:
                        df_chunk = pd.DataFrame(data[:remaining], columns=["movie_id", "customer_id", "rating", "date"])
                        # descartar el excedente y marcar tope
                        data = []
                        llego_al_tope = True
                    else:
                        df_chunk = pd.DataFrame(data, columns=["movie_id", "customer_id", "rating", "date"])
                        data = []

                    df_chunk.to_csv(
                        salida_csv_temp,
                        mode=writer_mode,
                        header=(writer_mode == 'w'),
                        index=False,
                        lineterminator="\n",
                    )
                    writer_mode = 'a'

                    escritos = len(df_chunk)
                    ratings_generados += escritos
                    remaining = TOPE_FILAS - ratings_generados

                    if remaining <= 0:
                        llego_al_tope = True
                        break

            line_count += 1
            if line_count % 1_000_000 == 0:
                print(f"  Procesadas {line_count:,} líneas de {ruta} (ratings acumulados: {ratings_generados:,})")

    # Volcar lo que quedó en buffer sin pasar el tope
    if not llego_al_tope and data:
        if len(data) > remaining:
            df_chunk = pd.DataFrame(data[:remaining], columns=["movie_id", "customer_id", "rating", "date"])
            llego_al_tope = True
        else:
            df_chunk = pd.DataFrame(data, columns=["movie_id", "customer_id", "rating", "date"])

        df_chunk.to_csv(
            salida_csv_temp,
            mode=writer_mode,
            header=(writer_mode == 'w'),
            index=False,
            lineterminator="\n",
        )
        ratings_generados += len(df_chunk)

    print(f"  ✓ Archivo {ruta} procesado. Ratings acumulados: {ratings_generados:,}")
    return ratings_generados, llego_al_tope

# Eliminar archivo temporal si existe
if os.path.exists(salida_csv_temp):
    os.remove(salida_csv_temp)

print("\n[1/5] Extrayendo ratings de archivos Netflix (tope = 1,000,000)...")
ratings_generados = 0
for i, archivo in enumerate(archivos_netflix):
    if ratings_generados >= TOPE_FILAS:
        print("  ▷ Tope alcanzado; se detiene la extracción.")
        break
    modo = 'w' if i == 0 else 'a'
    ratings_generados, tope = procesar_archivo_netflix(
        archivo, writer_mode=modo, ratings_generados=ratings_generados, TOPE_FILAS=TOPE_FILAS
    )
    if tope:
        print("  ▷ Tope alcanzado; no se procesan más archivos.")
        break

print(f"✅ Ratings extraídos (temporal): {ratings_generados:,} registros (máx {TOPE_FILAS:,})")

# ================================================================
# PASO 2: CREAR DIMENSIÓN PELÍCULA (dim_pelicula.csv)
# ================================================================

print("\n[2/5] Creando dimensión PELÍCULA...")

def leer_titulos_con_comas(archivo_titulos):
    """Lee el archivo de títulos manejando comas en los nombres"""
    data = []
    with open(archivo_titulos, encoding="latin-1") as f:
        for line in f:
            partes = line.strip().split(",", 2)
            if len(partes) == 3:
                movie_id, year, movie_name = partes
                if year.isdigit():
                    y = int(float(year))  # por si vienen como 1999.0
                else:
                    y = 1800
                data.append([int(movie_id), int(y), movie_name.strip()])
    return pd.DataFrame(data, columns=["movie_id", "year_of_release", "title"])

dim_pelicula = leer_titulos_con_comas(archivo_titulos)
dim_pelicula.to_csv(
    f"{OUTPUT_DIR}/dim_pelicula.csv",
    index=False,
    sep=",",
    encoding="utf-8",
    lineterminator="\n",
    quoting=2,
    quotechar='"',
    doublequote=True,
)
print(f"  ✓ Dimensión Película creada: {len(dim_pelicula)} películas")
print(f"  → {OUTPUT_DIR}/dim_pelicula.csv")

# ================================================================
# PASO 3: CREAR DIMENSIÓN USUARIO (dim_usuario.csv)
# ================================================================

print("\n[3/5] Creando dimensión USUARIO...")

# Obtener usuarios únicos del archivo temporal
usuarios_unicos = set()
for chunk in pd.read_csv(salida_csv_temp, chunksize=chunk_size, usecols=['customer_id']):
    usuarios_unicos.update(chunk['customer_id'].unique())

dim_usuario = pd.DataFrame(sorted(usuarios_unicos), columns=['customer_id'])
dim_usuario.to_csv(f"{OUTPUT_DIR}/dim_usuario.csv", index=False)
print(f"  ✓ Dimensión Usuario creada: {len(dim_usuario)} usuarios")
print(f"  → {OUTPUT_DIR}/dim_usuario.csv")

# ================================================================
# PASO 4: CREAR DIMENSIÓN FECHA (dim_fecha.csv)
# ================================================================

print("\n[4/5] Creando dimensión FECHA...")

# Obtener fechas únicas del archivo temporal
fechas_unicas = set()
for chunk in pd.read_csv(salida_csv_temp, chunksize=chunk_size, usecols=['date']):
    fechas_unicas.update(chunk['date'].unique())

# Crear dimensión fecha con atributos derivados
fecha_data = []
for fecha_str in sorted(fechas_unicas):
    try:
        fecha = pd.to_datetime(fecha_str)
        date_key = int(fecha.strftime("%Y%m%d"))
        
        fecha_data.append({
            'date_key': date_key,
            'full_date': fecha.strftime("%Y-%m-%d"),
            'year': fecha.year,
            'quarter': (fecha.month - 1) // 3 + 1,
            'month': fecha.month,
            'day': fecha.day,
            'week_of_year': fecha.isocalendar()[1]
        })
    except:
        continue

dim_fecha = pd.DataFrame(fecha_data)
dim_fecha.to_csv(f"{OUTPUT_DIR}/dim_fecha.csv", index=False)
print(f"  ✓ Dimensión Fecha creada: {len(dim_fecha)} fechas únicas")
print(f"  → {OUTPUT_DIR}/dim_fecha.csv")

# ================================================================
# PASO 5: CREAR TABLA DE HECHOS (fact_rating.csv) - TOPE
# ================================================================

print("\n[5/5] Creando tabla de HECHOS (Ratings)...")

# Crear diccionario de fechas para mapping
fecha_mapping = dict(zip(dim_fecha['full_date'], dim_fecha['date_key']))

# Procesar ratings y crear tabla de hechos
fact_ratings_list = []
total_procesados = 0

for chunk in pd.read_csv(salida_csv_temp, chunksize=chunk_size):
    # Convertir fecha a date_key
    chunk['date'] = pd.to_datetime(chunk['date'], errors='coerce')
    chunk['date_key'] = chunk['date'].dt.strftime("%Y-%m-%d").map(fecha_mapping)

    fact_chunk = chunk[['customer_id', 'movie_id', 'date_key', 'rating']].copy()
    fact_chunk.rename(columns={'rating': 'rating_value'}, inplace=True)
    fact_chunk = fact_chunk.dropna()

    # Recortar si por algún motivo superara el tope (defensivo)
    restante = TOPE_FILAS - total_procesados
    if restante <= 0:
        break
    if len(fact_chunk) > restante:
        fact_chunk = fact_chunk.iloc[:restante]

    # Guardar chunk
    if total_procesados == 0:
        fact_chunk.to_csv(f"{OUTPUT_DIR}/fact_rating.csv", mode='w', header=True, index=False)
    else:
        fact_chunk.to_csv(f"{OUTPUT_DIR}/fact_rating.csv", mode='a', header=False, index=False)

    total_procesados += len(fact_chunk)
    print(f"  Procesados {total_procesados:,} ratings...")

print(f"  ✓ Tabla de Hechos creada: {total_procesados:,} registros (máx {TOPE_FILAS:,})")
print(f"  → {OUTPUT_DIR}/fact_rating.csv")

# ================================================================
# LIMPIEZA
# ================================================================

print("\n[Limpieza] Eliminando archivos temporales...")
if os.path.exists(salida_csv_temp):
    os.remove(salida_csv_temp)
    print("  ✓ Archivo temporal eliminado")

# ================================================================
# RESUMEN FINAL
# ================================================================

print("\n" + "=" * 60)
print("✅ PROCESO ETL COMPLETADO EXITOSAMENTE")
print("=" * 60)
print("\nArchivos generados:")
print(f"  1. {OUTPUT_DIR}/dim_pelicula.csv  - {len(dim_pelicula):,} películas")
print(f"  2. {OUTPUT_DIR}/dim_usuario.csv   - {len(dim_usuario):,} usuarios")
print(f"  3. {OUTPUT_DIR}/dim_fecha.csv     - {len(dim_fecha):,} fechas")
print(f"  4. {OUTPUT_DIR}/fact_rating.csv   - {total_procesados:,} ratings (tope)")
print("\nPróximo paso: Ejecutar load_to_mysql.sql para cargar a MySQL")
print("=" * 60)
