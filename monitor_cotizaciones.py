import pyodbc
import pandas as pd
import sys
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "monitor_cotizaciones.xlsx")

def conectar_sql(server, database, user, password):
    conn_str = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={server};DATABASE={database};UID={user};PWD={password};'
        f'Encrypt=yes;TrustServerCertificate=yes;Connection Timeout=60;'
    )
    try:
        return pyodbc.connect(conn_str, timeout=60)
    except Exception as e:
        print(f"Error conexión: {e}")
        sys.exit(1)

conn1 = conectar_sql("10.10.5.5", "ELTORNILLO", "USER", "US3RT0R_$$")
conn2 = conectar_sql("eltornilloaws.ddns.net", "NASSER", "USER", "US3RT0R")
conn3 = conectar_sql("eltornilloaws.ddns.net", "TDL", "USER", "US3RT0R")

if len(sys.argv) < 3:
    print("Uso: python monitor_cotizaciones.py 2025-11-01 2025-11-10")
    sys.exit(1)

fechain = f"'{sys.argv[1]}'"
fechafin = f"'{sys.argv[2]}'"

# === QUERY TOR COMPLETA (copia-pega tu versión original, solo cambia Margen_Aux a '35') ===
query_tor = f"""
WITH datos_base AS (
    SELECT 'TOR' AS origen, T1.[sucursal], T1.[fol], T1.Documento, T1.Fecha,
           TRIM(T1.[Clave Kepler]) AS Clave_Kepler, T1.Descripcion, T1.Cantidad, T1.Unidad,
           T1.Precio, T1.[Venta Total] AS Venta_Total, T1.[Lista Cliente], T1.Lista_Precio, T1.Usuario,
           T1.Cliente, T1.[Razon Social] AS Razon_Social, T1.TMK, T1.[Costo Real], T1.[Porcentaje Desc. Costo Real],
           T1.L10, T1.Partida
    FROM Margen_Aux({fechain}, {fechafin},'','U','D','35','','',0,0,0) T1
),
datos_base_torr AS (
    SELECT 'TORR' AS origen, T1.[sucursal], T1.[fol], T1.Documento, T1.Fecha,
           TRIM(T1.[Clave Kepler]) AS Clave_Kepler, T1.Descripcion, T1.Cantidad, T1.Unidad,
           T1.Precio, T1.[Venta Total] AS Venta_Total, T1.[Lista Cliente], T1.Lista_Precio, T1.Usuario,
           T1.Cliente, T1.[Razon Social] AS Razon_Social, T1.TMK, T1.[Costo Real], T1.[Porcentaje Desc. Costo Real],
           T1.L10, T1.Partida
    FROM Margen_Aux({fechain}, {fechafin},'','U','D','35','','',0,0,0) T1
),
-- [EL RESTO DE TU CTE IDÉNTICO AL ORIGINAL]
joined AS (
    -- [tu lógica completa de joins]
    SELECT d.*, ISNULL(li.c4, 0) AS costoimp, /* etc */
    FROM (SELECT * FROM datos_base UNION ALL SELECT * FROM datos_base_torr) d
    LEFT JOIN /* tus joins */
)
SELECT d.origen + d.sucursal AS sucursal, /* todo tu SELECT final */
FROM joined d
"""

# === HAZ LO MISMO CON query_nas_tdl === 
query_nas_tdl = f"""
-- [copia-pega tu query original de NAS/TDL con '35']
"""

try:
    print("Ejecutando TOR...")
    df_tor = pd.read_sql(query_tor, conn1)
    df_nas = pd.read_sql(query_nas_tdl.replace("{prefix}", "NAS"), conn2)
    df_tdl = pd.read_sql(query_nas_tdl.replace("{prefix}", "TDL"), conn3)

    df = pd.concat([df_tor, df_nas, df_tdl], ignore_index=True)
    df = df[~df["Documento"].astype(str).str.contains("Re-Facturacion", na=False)]
    df = df.drop_duplicates(subset=['Documento', 'Clave_Kepler', 'fol'])

    COLUMNAS = ['sucursal', 'fol', 'Documento', 'Fecha', 'Clave_Kepler', 'Descripcion',
                'Cantidad', 'Unidad', 'Precio', 'Venta_Total', 'Lista Cliente', 'Lista_Precio',
                'Usuario', 'Cliente', 'Razon_Social', 'TMK', 'Costo Real', 'Porcentaje Desc. Costo Real',
                'L10', 'costoimp', 'Partida', 'CREAL', 'NGMX', 'Costo_Lista_Cliente']

    df = df[COLUMNAS]
    df_bajo = df[df["CREAL"] == "BAJO"]

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, "Todo", index=False)
        df_bajo.to_excel(writer, "Solo BAJO", index=False)

    print(f"EXCEL GENERADO: {OUTPUT_FILE}")
    print(f"Total Cotizado: ${df['Venta_Total'].sum():,.0f}")
except Exception as e:
    print(f"ERROR: {e}")
    sys.exit(1)
