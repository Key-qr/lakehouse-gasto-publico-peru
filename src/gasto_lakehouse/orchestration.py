"""Orquestación con Dagster (software-defined assets).

Sin ``from __future__ import annotations``: Dagster inspecciona los tipos en runtime.

Cada capa del lakehouse es un asset; Dagster dibuja el linaje, guarda el historial de
materializaciones y corre los asset checks de calidad y reconciliación. La lógica vive en
``pipeline``: este módulo solo la expone, así CLI y Dagster ejecutan exactamente el mismo código.

    dagster dev -m gasto_lakehouse.orchestration
"""

import uuid
from pathlib import Path

import polars as pl
from dagster import (
    AssetCheckResult,
    AssetCheckSeverity,
    AssetExecutionContext,
    AssetSelection,
    ConfigurableResource,
    DefaultScheduleStatus,
    DefaultSensorStatus,
    Definitions,
    MaterializeResult,
    MetadataValue,
    RunRequest,
    ScheduleDefinition,
    SensorEvaluationContext,
    SkipReason,
    asset,
    asset_check,
    define_asset_job,
    sensor,
)

from gasto_lakehouse import export, pipeline, storage
from gasto_lakehouse.config import Settings

GROUP = "lakehouse_gasto"


def _md_table(rows: list[dict]) -> str:
    cols = list(rows[0])
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    lines += [
        "| "
        + " | ".join(
            f"{r[c]:,}" if isinstance(r[c], int) and c != "año" else str(r[c]) for c in cols
        )
        + " |"
        for r in rows
    ]
    return "\n".join(lines)


class LakehouseResource(ConfigurableResource):
    root: str = "data"
    max_error_ratio: float = 0.05

    def settings(self) -> Settings:
        base = Settings.from_env()
        return Settings(
            root=Path(self.root).resolve(),
            sources_file=base.sources_file,
            max_error_ratio=self.max_error_ratio,
            budget_semantics=base.budget_semantics,
        )


@asset(group_name=GROUP, compute_kind="pyarrow", description="CSV del MEF -> Delta (texto crudo)")
def bronze_gasto(context: AssetExecutionContext, lakehouse: LakehouseResource) -> MaterializeResult:
    s = lakehouse.settings()
    s.ensure_dirs()
    results = pipeline.stage_bronze(s)
    new = [r for r in results if not r.skipped]
    for r in new:
        context.log.info(f"{r.source_file}: {r.rows:,} filas (batch {r.batch_id})")
    return MaterializeResult(
        metadata={
            "archivos_nuevos": len(new),
            "filas_nuevas": sum(r.rows for r in new),
            "archivos_omitidos": MetadataValue.json([r.source_file for r in results if r.skipped]),
        }
    )


@asset(
    group_name=GROUP,
    deps=[bronze_gasto],
    compute_kind="polars",
    description="Tipado, limpieza, cuarentena y deduplicación por año",
)
def silver_gasto(context: AssetExecutionContext, lakehouse: LakehouseResource) -> MaterializeResult:
    s = lakehouse.settings()
    pending = pipeline.pending_silver(s)
    results = pipeline.stage_silver(s, uuid.uuid4().hex, pending)
    rows = [
        {
            "año": r.year,
            "entrada": r.rows_in,
            "cuarentena": r.rows_quarantined,
            "duplicados": r.duplicates_removed,
            "salida": r.rows_out,
        }
        for r in results
    ]
    md = _md_table(rows) if rows else "Sin años pendientes"
    return MaterializeResult(metadata={"resumen": MetadataValue.md(md), "años": len(results)})


@asset(
    group_name=GROUP,
    deps=[silver_gasto],
    compute_kind="polars",
    description="Dimensiones SCD1 y dim_entidad SCD2",
)
def gold_dimensiones(lakehouse: LakehouseResource) -> MaterializeResult:
    s = lakehouse.settings()
    pipeline.stage_gold_dims(s)
    versions = storage.read(s.table("gold", "dim_entidad"))
    return MaterializeResult(
        metadata={
            "entidades": versions["sec_ejec"].n_unique(),
            "versiones_entidad": versions.height,
        }
    )


@asset(
    group_name=GROUP,
    deps=[gold_dimensiones],
    compute_kind="polars",
    description="fact_ejecucion_mensual y fact_presupuesto_anual + reconciliación",
)
def gold_hechos(lakehouse: LakehouseResource) -> MaterializeResult:
    s = lakehouse.settings()
    years = pipeline.pending_gold(s)
    rec = pipeline.stage_gold_facts(s, years)
    return MaterializeResult(
        metadata={"años": sorted(years), "reconciliacion": MetadataValue.json(rec)}
    )


@asset(
    group_name=GROUP,
    deps=[gold_hechos],
    compute_kind="duckdb",
    description="warehouse.duckdb con gold + marts SQL",
)
def warehouse_duckdb(lakehouse: LakehouseResource) -> MaterializeResult:
    path = export.build_warehouse(lakehouse.settings())
    return MaterializeResult(
        metadata={"ruta": str(path), "mb": round(path.stat().st_size / 1e6, 2)}
    )


@asset_check(asset=silver_gasto, description="Ninguna regla de severidad error supera el umbral")
def calidad_silver(lakehouse: LakehouseResource) -> AssetCheckResult:
    s = lakehouse.settings()
    path = s.table("_meta", "quality_results")
    if not storage.exists(path):
        return AssetCheckResult(passed=True, metadata={"nota": "sin evaluaciones aún"})
    df = storage.read(path)
    last = df.sort("evaluado_en")["run_id"][-1]
    cur = df.filter(pl.col("run_id") == last)
    worst = cur.filter(pl.col("severidad") == "error")["tasa_cumplimiento"].min()
    warn_fail = int(cur.filter(pl.col("severidad") == "warn")["filas_fallidas"].sum())
    return AssetCheckResult(
        passed=worst is None or (1 - worst) <= s.max_error_ratio,
        severity=AssetCheckSeverity.ERROR,
        metadata={"peor_tasa_cumplimiento": float(worst or 1.0), "filas_warn": warn_fail},
    )


@asset_check(asset=gold_hechos, description="Devengado de gold cuadra con silver por año")
def reconciliacion_gold(lakehouse: LakehouseResource) -> AssetCheckResult:
    from gasto_lakehouse import gold, silver

    s = lakehouse.settings()
    years = sorted(silver.published_batches(s))
    try:
        for y in years:
            gold.reconcile(s, y)
        return AssetCheckResult(passed=True, metadata={"años": years})
    except AssertionError as exc:
        return AssetCheckResult(passed=False, metadata={"error": str(exc)})


pipeline_job = define_asset_job("pipeline_completo", selection=AssetSelection.groups(GROUP))

# El MEF actualiza los archivos del año en curso cada mes: corrida el día 5 a las 6:00 (Lima).
monthly = ScheduleDefinition(
    job=pipeline_job,
    cron_schedule="0 6 5 * *",
    execution_timezone="America/Lima",
    default_status=DefaultScheduleStatus.STOPPED,
)


@sensor(job=pipeline_job, minimum_interval_seconds=60, default_status=DefaultSensorStatus.STOPPED)
def nuevos_archivos_landing(context: SensorEvaluationContext, lakehouse: LakehouseResource):
    """Dispara el pipeline cuando aparece o cambia un CSV en landing."""
    files = pipeline.landing_files(lakehouse.settings())
    if not files:
        return SkipReason("landing vacío")
    fingerprint = "|".join(f"{f.name}:{f.stat().st_size}:{int(f.stat().st_mtime)}" for f in files)
    if fingerprint == context.cursor:
        return SkipReason("sin cambios en landing")
    context.update_cursor(fingerprint)
    return RunRequest(run_key=fingerprint)


defs = Definitions(
    assets=[bronze_gasto, silver_gasto, gold_dimensiones, gold_hechos, warehouse_duckdb],
    asset_checks=[calidad_silver, reconciliacion_gold],
    jobs=[pipeline_job],
    schedules=[monthly],
    sensors=[nuevos_archivos_landing],
    resources={"lakehouse": LakehouseResource(root=str(Settings.from_env().root))},
)
