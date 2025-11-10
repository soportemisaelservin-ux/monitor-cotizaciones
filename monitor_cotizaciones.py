# monitor_cotizaciones.py
import pyodbc
import pandas as pd
import sys
import os
# === FIX PARA STREAMLIT CLOUD - DRIVER SQL SERVER ===
import os
if os.environ.get('STREAMLIT') == 'cloud':
    # Forza la instalación del driver en Linux
    os.system("apt-get update && apt-get install -y unixodbc-dev")
    os.system("curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -")
    os.system("curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list > /etc/apt/sources.list.d/mssql-release.list")
    os.system("apt-get update")
    os.system("ACCEPT_EULA=Y apt-get install -y msodbcsql18")
# =====================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, "monitor_cotizaciones.xlsx")

def conectar_sql(server, database, user, password):
    conn_str = (
        f'DRIVER={{ODBC Driver 17 for SQL Server}};'
        f'SERVER={server};DATABASE={database};UID={user};PWD={password};'
        f'TrustServerCertificate=yes;'
    )
    try:
        return pyodbc.connect(conn_str)
    except Exception as e:
        print(f"Error conectando a {database}: {e}")
        sys.exit(1)

# === CONEXIONES ===
conn1 = conectar_sql("10.10.5.5", "ELTORNILLO", "USER", "US3RT0R_$$")
conn2 = conectar_sql("eltornilloaws.ddns.net", "NASSER", "USER", "US3RT0R")
conn3 = conectar_sql("eltornilloaws.ddns.net", "TDL", "USER", "US3RT0R")

if len(sys.argv) < 3:
    print("Uso: python monitor_cotizaciones.py 2025-11-01 2025-11-10")
    sys.exit(1)

fechain = f"'{sys.argv[1]}'"
fechafin = f"'{sys.argv[2]}'"

# === QUERY PARA TOR (COTIZACIONES CON 35) ===
query_tor = f"""
WITH datos_base AS (
    SELECT 
        'TOR' AS origen,
        T1.[sucursal], T1.[fol], T1.Documento, T1.Fecha,
        TRIM(T1.[Clave Kepler]) AS Clave_Kepler, T1.Descripcion, T1.Cantidad, T1.Unidad,
        T1.Precio, T1.[Venta Total] AS Venta_Total, T1.[Lista Cliente], T1.Lista_Precio, T1.Usuario,
        T1.Cliente, T1.[Razon Social] AS Razon_Social, T1.TMK, T1.[Costo Real], T1.[Porcentaje Desc. Costo Real],
        T1.L10, T1.Partida
    FROM Margen_Aux({fechain}, {fechafin},'','U','D','35','','',0,0,0) T1
),
datos_base_torr AS (
    SELECT 
        'TORR' AS origen,
        T1.[sucursal], T1.[fol], T1.Documento, T1.Fecha,
        TRIM(T1.[Clave Kepler]) AS Clave_Kepler, T1.Descripcion, T1.Cantidad, T1.Unidad,
        T1.Precio, T1.[Venta Total] AS Venta_Total, T1.[Lista Cliente], T1.Lista_Precio, T1.Usuario,
        T1.Cliente, T1.[Razon Social] AS Razon_Social, T1.TMK, T1.[Costo Real], T1.[Porcentaje Desc. Costo Real],
        T1.L10, T1.Partida
    FROM Margen_Aux({fechain}, {fechafin},'','U','D','35','','',0,0,0) T1
),
costo_reciente AS (
    SELECT c1, c4, c5, c6, c7, c8,
           ROW_NUMBER() OVER (PARTITION BY c1, c4 ORDER BY CAST(c2 AS date) DESC, c8 DESC) AS rn
    FROM BDHISTLISTCR 
),
listimp_reciente AS (
    SELECT c1, c4, c5, c6,
           ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY CAST(c2 AS date) DESC) AS rn
    FROM bdhistlistimp WHERE c3 NOT IN ('JPEREZ','LZAVALA','JTORRES')
),
listpre_reciente AS (
    SELECT c1, c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c18,c19,
           ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY c3 DESC) AS rn
    FROM bdhistlist
),
listep_reciente AS (
    SELECT c1, c4, c5,
           ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY CAST(c2 AS date) DESC) AS rn
    FROM bdhistlistesp
),
joined AS (
    SELECT 
        d.*,
        ISNULL(li.c4, 0) AS costoimp,
        ISNULL(lp.c5, 0) AS c5, ISNULL(lp.c6, 0) AS c6, ISNULL(lp.c7, 0) AS c7, ISNULL(lp.c8, 0) AS c8,
        ISNULL(lp.c9, 0) AS c9, ISNULL(lp.c10, 0) AS c10, ISNULL(lp.c11, 0) AS c11, ISNULL(lp.c12, 0) AS c12,
        ISNULL(lp.c13, 0) AS c13, ISNULL(lp.c14, 0) AS c14, ISNULL(lp.c18, 0) AS c18, ISNULL(lp.c19, 0) AS c19,
        ISNULL(li.c5, 0) AS listimp_c5, ISNULL(li.c6, 0) AS listimp_c6,
        ISNULL(le.c4, 0) AS listep_c4, ISNULL(le.c5, 0) AS listep_c5
    FROM (
        SELECT * FROM datos_base
        UNION ALL
        SELECT * FROM datos_base_torr
    ) d
    LEFT JOIN costo_reciente cr ON cr.c1 = d.Clave_Kepler AND cr.c4 = d.[Costo Real] AND cr.rn = 1
    LEFT JOIN listimp_reciente li ON li.c1 = d.Clave_Kepler AND li.rn = 1
    LEFT JOIN listpre_reciente lp ON lp.c1 = d.Clave_Kepler AND lp.rn = 1
    LEFT JOIN listep_reciente le ON le.c1 = d.Clave_Kepler AND le.rn = 1
)

SELECT 
    d.origen + d.sucursal AS sucursal,
    d.fol, d.Documento, d.Fecha, d.Clave_Kepler, d.Descripcion, d.Cantidad, d.Unidad, d.Precio, d.Venta_Total,
    d.[Lista Cliente], d.Lista_Precio, d.Usuario, d.Cliente, d.Razon_Social, d.TMK, d.[Costo Real],
    d.[Porcentaje Desc. Costo Real], d.L10, d.costoimp, d.Partida,
    CASE WHEN d.[Porcentaje Desc. Costo Real] < 0.2499 THEN 'BAJO' ELSE '' END AS CREAL,
    CASE 
        WHEN d.Precio = 0 THEN 'OTROS'
        WHEN d.costoimp = 0 THEN 'OTROS'
        WHEN (d.Precio - d.costoimp) / NULLIF(d.Precio, 0) < 0.38 THEN 'BAJO'
        ELSE 'ALTO'
    END AS NGMX,
    CASE 
        TRY_CAST(LEFT(CAST(d.[Lista Cliente] AS VARCHAR(20)), 
            ISNULL(NULLIF(PATINDEX('%[^0-9]%', CAST(d.[Lista Cliente] AS VARCHAR(20)) + ' '), 0) - 1, 20)) AS INT)
        WHEN 1 THEN d.c5 WHEN 2 THEN d.c6 WHEN 3 THEN d.c7 WHEN 4 THEN d.c8
        WHEN 5 THEN d.c9 WHEN 6 THEN d.c10 WHEN 7 THEN d.c11 WHEN 8 THEN d.c12
        WHEN 9 THEN d.c13 WHEN 10 THEN d.c14 WHEN 11 THEN d.listimp_c5
        WHEN 12 THEN d.listimp_c6 WHEN 13 THEN d.listep_c4 WHEN 14 THEN d.listep_c5
        WHEN 15 THEN d.c18 WHEN 16 THEN d.c19
        ELSE NULL
    END AS Costo_Lista_Cliente
FROM joined d
"""

# === QUERY PARA NAS Y TDL ===
query_nas_tdl = f"""
WITH datos_base AS (
    SELECT 
        '{{prefix}}' AS origen,
        T1.[sucursal], T1.[fol], T1.Documento, T1.Fecha,
        TRIM(T1.[Clave Kepler]) AS Clave_Kepler, T1.Descripcion, T1.Cantidad, T1.Unidad,
        T1.Precio, T1.[Venta Total] AS Venta_Total, T1.[Lista Cliente], T1.Lista_Precio, T1.Usuario,
        T1.Cliente, T1.[Razon Social] AS Razon_Social, T1.TMK, T1.[Costo Real], T1.[Porcentaje Desc. Costo Real],
        T1.L10, T1.Partida
    FROM Margen_Aux({fechain}, {fechafin},'','U','D','35','','',0,0,0) T1
),
listimp_reciente AS (
    SELECT c1, c4, c5, c6,
           ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY CAST(c2 AS date) DESC) AS rn
    FROM bdhistlistimp WHERE c3 NOT IN ('JPEREZ','LZAVALA','JTORRES')
),
listpre_reciente AS (
    SELECT c1, c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c18,c19,
           ROW_NUMBER() OVER (PARTITION BY c1 ORDER BY c3 DESC) AS rn
    FROM bdhistlist
)
SELECT 
    origen + sucursal AS sucursal,
    fol, Documento, Fecha, Clave_Kepler, Descripcion, Cantidad, Unidad, Precio, Venta_Total,
    [Lista Cliente], Lista_Precio, Usuario, Cliente, Razon_Social, TMK, [Costo Real],
    [Porcentaje Desc. Costo Real], L10,
    ISNULL(li.c4, 0) AS costoimp,
    Partida,
    CASE WHEN [Porcentaje Desc. Costo Real] < 0.2499 THEN 'BAJO' ELSE '' END AS CREAL,
    CASE 
        WHEN Precio = 0 THEN 'OTROS'
        WHEN ISNULL(li.c4, 0) = 0 THEN 'OTROS'
        WHEN (Precio - ISNULL(li.c4, 0)) / NULLIF(Precio, 0) < 0.38 THEN 'BAJO'
        ELSE 'ALTO'
    END AS NGMX,
    CASE 
        TRY_CAST(LEFT(CAST([Lista Cliente] AS VARCHAR(20)), 
            ISNULL(NULLIF(PATINDEX('%[^0-9]%', CAST([Lista Cliente] AS VARCHAR(20)) + ' '), 0) - 1, 20)) AS INT)
        WHEN 1 THEN lp.c5 WHEN 2 THEN lp.c6 WHEN 3 THEN lp.c7 WHEN 4 THEN lp.c8
        WHEN 5 THEN lp.c9 WHEN 6 THEN lp.c10 WHEN 7 THEN lp.c11 WHEN 8 THEN lp.c12
        WHEN 9 THEN lp.c13 WHEN 10 THEN lp.c14 WHEN 11 THEN li.c5 WHEN 12 THEN li.c6
        WHEN 15 THEN lp.c18 WHEN 16 THEN lp.c19
        ELSE NULL
    END AS Costo_Lista_Cliente
FROM datos_base d
LEFT JOIN listimp_reciente li ON li.c1 = d.Clave_Kepler AND li.rn = 1
LEFT JOIN listpre_reciente lp ON lp.c1 = d.Clave_Kepler AND lp.rn = 1
"""

# === EJECUCIÓN ===
try:
    print("Ejecutando TOR...")
    df_tor = pd.read_sql(query_tor, conn1)
    print(f"TOR: {len(df_tor)} filas")

    print("Ejecutando NAS...")
    df_nas = pd.read_sql(query_nas_tdl.replace("{prefix}", "NAS"), conn2)
    print(f"NAS: {len(df_nas)} filas")

    print("Ejecutando TDL...")
    df_tdl = pd.read_sql(query_nas_tdl.replace("{prefix}", "TDL"), conn3)
    print(f"TDL: {len(df_tdl)} filas")

    dfs = [df for df in [df_tor, df_nas, df_tdl] if not df.empty]
    df = pd.concat(dfs, ignore_index=True)
    df = df[~df["Documento"].astype(str).str.contains("Re-Facturacion", na=False)]
    df = df.drop_duplicates(subset=['Documento', 'Clave_Kepler', 'fol'])

    COLUMNAS = ['sucursal', 'fol', 'Documento', 'Fecha', 'Clave_Kepler', 'Descripcion',
                'Cantidad', 'Unidad', 'Precio', 'Venta_Total', 'Lista Cliente', 'Lista_Precio',
                'Usuario', 'Cliente', 'Razon_Social', 'TMK', 'Costo Real', 'Porcentaje Desc. Costo Real',
                'L10', 'costoimp', 'Partida', 'CREAL', 'NGMX', 'Costo_Lista_Cliente']

    for col in COLUMNAS:
        if col not in df.columns:
            df[col] = pd.NA
    df = df[COLUMNAS]
    df_bajo = df[df["CREAL"] == "BAJO"]

    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        df.to_excel(writer, "Todo", index=False)
        df_bajo.to_excel(writer, "Solo BAJO", index=False)
    print(f"EXCEL GENERADO: {OUTPUT_FILE}")
    print(f"Total Cotizado: ${df['Venta_Total'].sum():,.0f}")
    print(f"CREAL BAJO: ${df_bajo['Venta_Total'].sum():,.0f}")
except Exception as e:
    print(f"ERROR: {e}")

    sys.exit(1)
