"""Publicación de gold a DuckDB y capa semántica en SQL.

El lakehouse (Delta) es la fuente de verdad; ``warehouse.duckdb`` es un artefacto derivado y
desechable para consumo: dashboard, notebooks o cualquier cliente SQL. Se construye en un
archivo temporal y se reemplaza al final, así un lector nunca ve un warehouse a medias.
"""

from __future__ import annotations

import logging
import os
from importlib import resources
from pathlib import Path

import duckdb

from gasto_lakehouse import storage
from gasto_lakehouse.config import Settings

log = logging.getLogger(__name__)

GOLD_TABLES = (
    "dim_tiempo",
    "dim_entidad",
    "dim_funcional",
    "dim_clasificador_gasto",
    "dim_fuente",
    "dim_ubigeo",
    "dim_producto_proyecto",
    "fact_ejecucion_mensual",
    "fact_presupuesto_anual",
)

MART_TABLES = (
    "avance_departamento",
    "avance_sector",
    "curva_ejecucion",
    "proyectos_inversion",
    "fuente_financiamiento",
)


def mart_sql_files() -> list[tuple[str, str]]:
    folder = resources.files("gasto_lakehouse") / "sql"
    files = sorted(f for f in folder.iterdir() if f.name.endswith(".sql"))
    return [(f.name, f.read_text(encoding="utf-8")) for f in files]


def build_warehouse(settings: Settings, target: Path | None = None) -> Path:
    target = Path(target or settings.warehouse_db)
    tmp = target.with_suffix(".building.duckdb")
    tmp.unlink(missing_ok=True)

    con = duckdb.connect(str(tmp))
    try:
        con.execute("CREATE SCHEMA gold; CREATE SCHEMA marts;")
        for name in GOLD_TABLES:
            path = settings.table("gold", name)
            if not storage.exists(path):
                raise FileNotFoundError(f"Falta la tabla gold {name}; corre el pipeline primero")
            arrow = storage.read(path).to_arrow()
            con.register("_src", arrow)
            con.execute(f"CREATE TABLE gold.{name} AS SELECT * FROM _src")
            con.unregister("_src")
            log.info("warehouse: gold.%s (%s filas)", name, arrow.num_rows)
        for fname, sql in mart_sql_files():
            con.execute(sql)
            log.info("warehouse: %s aplicado", fname)
        con.execute("CHECKPOINT")
    finally:
        con.close()

    os.replace(tmp, target)
    log.info("warehouse listo en %s", target)
    return target


def export_marts(warehouse: Path, out_dir: Path) -> list[Path]:
    """Exporta solo los marts a Parquet: pocos MB, aptos para versionar y para la demo pública."""
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(warehouse), read_only=True)
    paths = []
    try:
        for name in MART_TABLES:
            p = out_dir / f"{name}.parquet"
            con.execute(f"COPY marts.{name} TO '{p.as_posix()}' (FORMAT parquet, COMPRESSION zstd)")
            paths.append(p)
    finally:
        con.close()
    return paths
