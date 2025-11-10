# app.py
import streamlit as st
import pandas as pd
import subprocess
import os
from datetime import date

st.set_page_config(page_title="Cotizaciones El Tornillo", layout="wide")
st.title("MONITOR DE COTIZACIONES - EL TORNILLO")
st.markdown("**Sistema oficial - Dirección**")

hoy = date.today()
inicio = hoy.replace(day=1)
c1, c2 = st.columns(2)
f1 = c1.date_input("Desde", inicio)
f2 = c2.date_input("Hasta", hoy)

if st.button("GENERAR REPORTE", type="primary", use_container_width=True):
    with st.spinner("Generando..."):
        cmd = ["python", "monitor_cotizaciones.py", str(f1), str(f2)]
        r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode == 0:
        st.success("¡Listo!")
        st.balloons()
        df = pd.read_excel("monitor_cotizaciones.xlsx", "Todo")
        bajo = pd.read_excel("monitor_cotizaciones.xlsx", "Solo BAJO")
        t = df["Venta_Total"].sum()
        b = bajo["Venta_Total"].sum()
        col1, col2, col3 = st.columns(3)
        col1.metric("Total", f"${t:,.0f}")
        col2.metric("BAJO", f"${b:,.0f}")
        col3.metric("%", f"{b/t*100:.1f}%")
        with open("monitor_cotizaciones.xlsx", "rb") as f:
            st.download_button("DESCARGAR", f.read(), "cotizaciones.xlsx")
    else:
        st.error("Error")
        st.code(r.stderr)

if os.path.exists("monitor_cotizaciones.xlsx"):
    todo = pd.read_excel("monitor_cotizaciones.xlsx", "Todo")
    bajo = pd.read_excel("monitor_cotizaciones.xlsx", "Solo BAJO")
    t1, t2 = st.tabs(["TODAS", "SOLO BAJO"])
    with t1: st.dataframe(todo)
    with t2: st.dataframe(bajo.style.background_color('#ffcccc'))