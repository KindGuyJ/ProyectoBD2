import pandas as pd
from pandas import read_csv

# ================================
# ETAPA 1 - EXTRACT
# ================================
dft = pd.read_csv("combined_data_1.txt")
df = pd.concat
print("Datos originales:")
print(df)
# ================================
# ETAPA 2 - TRANSFORM
# ================================
# Normalizar nombres de columnas (ej: a minúsculas)
df.columns = [col.lower() for col in df.columns]
# Unificar formato de fechas
df['fecha_compra'] = pd.to_datetime(df['fecha_compra'], errors='coerce', dayfirst=True)
# Normalizar moneda a mayúsculas
df['moneda'] = df['moneda'].str.upper()
# Conversión de montos: supongamos USD → ARS (1 USD = 1000 ARS para el ejemplo)
df['monto_ars'] = df.apply(lambda row: row['monto'] * tasa_cambio if row['moneda'] == 'USD'
else row['monto'], axis=1)
# Completar valores faltantes (ej: nombre desconocido → "N/A")
df['nombre'] = df['nombre'].fillna("N/A")
# Crear un campo calculado: nombre completo
df['cliente'] = df['nombre'] + " " + df['apellido']
# Seleccionar solo las columnas limpias para el DW
df_limpio = df[['id_cliente','cliente','fecha_compra','producto','monto_ars']]
print("\nDatos transformados:")
print(df_limpio)
# ================================
# ETAPA 3 - LOAD
# ================================
# Guardar en un nuevo CSV limpio
df_limpio.to_csv("ventas_procesado.csv", index=False)
print("\nArchivo limpio generado: ventas_clean.csv")