## Proyecto
ETL y Data Warehouse - Netflix (Proyecto BD2)

Integrantes:
- Fernandez Favaron Juan Ignacio - 2307329
- [INTEGRANTE 2 - Nombre Apellido - Legajo]  <-- completar con datos del PDF
- [INTEGRANTE 3 - Nombre Apellido - Legajo]  <-- completar con datos del PDF

## Resumen
Este proyecto extrae, transforma y genera un pequeño data warehouse a partir
de los ficheros de ratings y títulos. Produce dimensiones (`dim_pelicula.csv`,
`dim_usuario.csv`, `dim_fecha.csv`) y la tabla de hechos (`fact_rating.csv`) en
la carpeta `output_dw`.

## Estructura relevante
- `scriptETL.py` : script principal en Python que genera los CSVs en `output_dw`.
- `archivosCSV/` : carpeta con los archivos de entrada (ratings y títulos).
- `sql_load_script.sql` : script SQL para crear las tablas y cargar los CSVs en MySQL.

## Requisitos
- Python 3.8+ (probado con Python 3.8/3.9)
- Pandas

Instalar dependencias (ejemplo):

```powershell
python -m pip install pandas
```

## Cómo ejecutar el ETL (generar CSVs)
1. Abrir un terminal en la carpeta del proyecto.
2. Ejecutar:

```powershell
python scriptETL.py
```

3. Al finalizar, los archivos generados estarán en `output_dw/`:
	- `dim_pelicula.csv`
	- `dim_usuario.csv`
	- `dim_fecha.csv`
	- `fact_rating.csv`

Notas sobre encoding y CSV:
- El script lee los archivos de entrada con `encoding='latin-1'` y escribe los
  CSVs con `encoding='utf-8'`. Esto es compatible con una tabla MySQL creada
  con `CHARSET=utf8mb4` si al importar indicás `CHARACTER SET utf8mb4`.
- Si vas a abrir `dim_pelicula.csv` en Excel (Windows) y querés que lo detecte
  correctamente, podés usar `encoding='utf-8-sig'` al escribir el CSV.

## Cómo cargar los CSVs a MySQL
1. Revisar la variable `secure_file_priv` en MySQL (si usás `LOAD DATA INFILE` sin
	`LOCAL` necesitas que los archivos estén en esa carpeta, el proyecto tiene hardcodeada la ubicacion de lectura, hay que modificar la ubicacion segun el dispositivo.):

```sql
SHOW VARIABLES LIKE 'secure_file_priv';
```

2. Ejecutar el script SQL `sql_load_script.sql`. Si usás `LOAD DATA LOCAL INFILE`
	desde la línea de comandos, invocá el cliente con `--local-infile=1`:

```powershell
mysql --local-infile=1 -u TU_USUARIO -p -D netflix < sql_load_script.sql
```

## Diagnóstico rápido si no se cargan filas
- Verificá que el CSV existe y tiene más de una línea (cabecera + datos).
- Ejecutá en MySQL justo después del `LOAD DATA`:

```sql
SELECT ROW_COUNT() AS filas_afectadas;
SHOW WARNINGS LIMIT 100;
SELECT COUNT(*) FROM dim_pelicula;
```

- Verificá `local_infile` y `secure_file_priv` si la carga no lee el fichero.

## Notas finales
- El script está pensado para un tope de 1,000,000 ratings (variable `TOPE_FILAS`).
---