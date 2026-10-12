"""Dashboard de avance de ejecución del gasto público.

Lee los marts que publica el pipeline. Orden de búsqueda de datos:
1. ``DASHBOARD_DB`` o ``data/warehouse.duckdb`` (lakehouse completo corrido en local).
2. ``dashboard/data/*.parquet`` (marts exportados con ``make demo-data``; es lo que usa la
   demo publicada, porque el warehouse completo no se versiona).

    streamlit run dashboard/app.py
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import plotly.express as px
import plotly.graph_objects as go
import polars as pl
import streamlit as st

HERE = Path(__file__).parent
DEMO_DIR = HERE / "data"
MARTS = (
    "avance_departamento",
    "avance_sector",
    "curva_ejecucion",
    "proyectos_inversion",
    "fuente_financiamiento",
)

# Tokens de color (paleta validada para daltonismo; ver docs/arquitectura.md).
SERIE = "#2a78d6"
SERIE_SUAVE = "#86b6ef"
TINTA = "#0b0b0b"
TINTA_2 = "#52514e"
EJE = "#898781"
GRID = "#e1e0d9"
SEMAFORO = {"ALTO": ("#0ca30c", "●"), "MEDIO": ("#fab219", "▲"), "BAJO": ("#d03b3b", "■")}

st.set_page_config(page_title="Gasto público Perú", page_icon="📊", layout="wide")


@st.cache_resource
def connection() -> tuple[duckdb.DuckDBPyConnection, str]:
    db = Path(os.getenv("DASHBOARD_DB", "data/warehouse.duckdb"))
    if db.exists():
        return duckdb.connect(str(db), read_only=True), f"warehouse local ({db})"
    con = duckdb.connect()
    con.execute("CREATE SCHEMA marts")
    missing = [m for m in MARTS if not (DEMO_DIR / f"{m}.parquet").exists()]
    if missing:
        st.error("No hay datos. Corre `make demo` o `lakehouse run` primero.")
        st.stop()
    for m in MARTS:
        con.execute(
            f"CREATE VIEW marts.{m} AS "
            f"SELECT * FROM read_parquet('{(DEMO_DIR / f'{m}.parquet').as_posix()}')"
        )
    meta_file = DEMO_DIR / "_meta.json"
    meta = json.loads(meta_file.read_text(encoding="utf-8")) if meta_file.exists() else {}
    return con, meta.get("fuente", "marts exportados")


def q(sql: str, params: list | None = None):
    con, _ = connection()
    return con.execute(sql, params or []).pl()


def soles(x: float) -> str:
    for div, suf in ((1e9, " mil millones"), (1e6, " millones"), (1e3, " mil")):
        if abs(x) >= div:
            return f"S/ {x / div:,.1f}{suf}"
    return f"S/ {x:,.0f}"


def style(fig: go.Figure, height: int = 380) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=16, t=8, b=8),
        plot_bgcolor="#fcfcfb",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(color=TINTA_2, size=13),
        hoverlabel=dict(bgcolor="white", font_color=TINTA),
        showlegend=False,
    )
    fig.update_xaxes(gridcolor=GRID, linecolor=EJE, zeroline=False, tickfont_color=EJE)
    fig.update_yaxes(gridcolor=GRID, linecolor=EJE, zeroline=False, tickfont_color=EJE)
    return fig


# ---------------------------------------------------------------------------------------------
_, fuente = connection()
anos = q("SELECT DISTINCT ano_eje FROM marts.avance_departamento ORDER BY 1 DESC")[
    "ano_eje"
].to_list()
niveles = q("SELECT DISTINCT nivel_gobierno FROM marts.avance_departamento ORDER BY 1")[
    "nivel_gobierno"
].to_list()

st.title("Ejecución del gasto público del Perú")
st.caption(f"Fuente: {fuente}. Avance = devengado / PIM.")

c1, c2 = st.columns([1, 3])
ano = c1.selectbox("Año", anos)
nivel_sel = c2.multiselect("Nivel de gobierno", niveles, default=niveles)
if not nivel_sel:
    st.info("Selecciona al menos un nivel de gobierno.")
    st.stop()

dep = q(
    "SELECT departamento, SUM(pim) pim, SUM(devengado) devengado, SUM(girado) girado "
    "FROM marts.avance_departamento WHERE ano_eje = ? AND list_contains(?, nivel_gobierno) "
    "GROUP BY 1",
    [ano, nivel_sel],
).with_columns(avance=(pl.col("devengado") / pl.col("pim") * 100).cast(pl.Float64).round(1))

pim, dev, gir = (float(dep[c].sum()) for c in ("pim", "devengado", "girado"))
k1, k2, k3, k4 = st.columns(4)
k1.metric("PIM", soles(pim))
k2.metric("Devengado", soles(dev))
k3.metric("Avance", f"{dev / pim * 100:,.1f} %" if pim else "-")
k4.metric("Girado", soles(gir))

# --- curva de ejecución ----------------------------------------------------------------------
st.subheader("Curva de ejecución acumulada")
curva = q(
    "SELECT mes, nombre_mes, avance_acumulado_pct, devengado_mes FROM marts.curva_ejecucion "
    "WHERE ano = ? AND mes > 0 ORDER BY mes",
    [ano],
)
fig = go.Figure(
    go.Scatter(
        x=curva["nombre_mes"],
        y=curva["avance_acumulado_pct"],
        mode="lines+markers",
        line=dict(color=SERIE, width=2),
        marker=dict(size=8, color=SERIE, line=dict(color="#fcfcfb", width=2)),
        customdata=[soles(float(v)) for v in curva["devengado_mes"]],
        hovertemplate=(
            "<b>%{x}</b><br>Avance acumulado: %{y:.1f}%<br>"
            "Devengado del mes: %{customdata}<extra></extra>"
        ),
    )
)
fig.update_yaxes(ticksuffix=" %", rangemode="tozero")
st.plotly_chart(style(fig, 320), width="stretch")
st.caption(
    "La curva suma todos los niveles de gobierno. El pico de diciembre es el patrón "
    "típico del gasto público peruano."
)

# --- departamentos ---------------------------------------------------------------------------
left, right = st.columns(2)
with left:
    st.subheader("Avance por departamento")
    d = dep.sort("avance")
    fig = px.bar(
        d,
        x="avance",
        y="departamento",
        orientation="h",
        custom_data=[
            [soles(float(v)) for v in d["pim"]],
            [soles(float(v)) for v in d["devengado"]],
        ],
    )
    fig.update_traces(
        marker_color=SERIE,
        marker_line_width=0,
        hovertemplate=(
            "<b>%{y}</b><br>Avance: %{x:.1f}%<br>PIM: %{customdata[0]}<br>"
            "Devengado: %{customdata[1]}<extra></extra>"
        ),
    )
    fig.update_xaxes(ticksuffix=" %", title=None)
    fig.update_yaxes(title=None)
    st.plotly_chart(style(fig, 640), width="stretch")

with right:
    st.subheader("Presupuesto por sector")
    sec = q(
        "SELECT sector, SUM(pim) pim, SUM(devengado) devengado FROM marts.avance_sector "
        "WHERE ano_eje = ? AND list_contains(?, nivel_gobierno) GROUP BY 1 ORDER BY pim",
        [ano, nivel_sel],
    )
    fig = px.bar(
        sec,
        x="pim",
        y="sector",
        orientation="h",
        custom_data=[
            [
                f"{float(a) / float(b) * 100:.1f}%" if b else "-"
                for a, b in zip(sec["devengado"], sec["pim"], strict=True)
            ]
        ],
    )
    fig.update_traces(
        marker_color=SERIE,
        hovertemplate="<b>%{y}</b><br>PIM: S/ %{x:,.0f}<br>Avance: %{customdata[0]}<extra></extra>",
    )
    fig.update_xaxes(title=None)
    fig.update_yaxes(title=None)
    st.plotly_chart(style(fig, 300), width="stretch")

    st.subheader("Fuente de financiamiento")
    fu = q(
        "SELECT fuente, SUM(pim) pim FROM marts.fuente_financiamiento "
        "WHERE ano_eje = ? GROUP BY 1 ORDER BY pim",
        [ano],
    )
    fig = px.bar(fu, x="pim", y="fuente", orientation="h")
    fig.update_traces(
        marker_color=SERIE_SUAVE, hovertemplate="<b>%{y}</b><br>PIM: S/ %{x:,.0f}<extra></extra>"
    )
    fig.update_xaxes(title=None)
    fig.update_yaxes(title=None)
    st.plotly_chart(style(fig, 280), width="stretch")

# --- proyectos -------------------------------------------------------------------------------
st.subheader("Proyectos de inversión")
pr = q(
    "SELECT codigo_proyecto, proyecto, entidad, departamento, nivel_gobierno, pim, devengado, "
    "avance_pct, semaforo FROM marts.proyectos_inversion "
    "WHERE ano_eje = ? AND list_contains(?, nivel_gobierno) ORDER BY pim DESC",
    [ano, nivel_sel],
)
cols = st.columns(3)
for col, nivel in zip(cols, ("BAJO", "MEDIO", "ALTO"), strict=True):
    color, icon = SEMAFORO[nivel]
    n = pr.filter(pl.col("semaforo") == nivel).height
    col.markdown(
        f"<span style='color:{color};font-size:1.2rem'>{icon}</span> "
        f"**Avance {nivel.lower()}**: {n} proyectos",
        unsafe_allow_html=True,
    )
st.dataframe(
    pr.head(200),
    width="stretch",
    hide_index=True,
    column_config={
        "pim": st.column_config.NumberColumn("PIM", format="S/ %.0f"),
        "devengado": st.column_config.NumberColumn("Devengado", format="S/ %.0f"),
        "avance_pct": st.column_config.ProgressColumn(
            "Avance", format="%.1f %%", min_value=0, max_value=100
        ),
    },
)

with st.expander("Ver tabla por departamento"):
    st.dataframe(dep.sort("avance", descending=True), width="stretch", hide_index=True)
