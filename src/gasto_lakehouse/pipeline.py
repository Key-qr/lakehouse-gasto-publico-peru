"""Orquestación de punta a punta: landing -> bronze -> silver -> gold -> warehouse.

Cada corrida deja un registro en ``_meta/pipeline_runs`` con su estado y métricas, haya
terminado bien o no. El pipeline es idempotente: correrlo dos veces seguidas con los mismos
archivos no cambia nada (bronze los reconoce por hash y no hay años nuevos que procesar).
"""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from gasto_lakehouse import bronze, export, gold, silver, storage
from gasto_lakehouse.config import Settings

log = logging.getLogger(__name__)

RUNS = ("_meta", "pipeline_runs")
LANDING_PATTERN = re.compile(r"^\d{4}-Gasto(-Mensual)?\.csv$", re.IGNORECASE)


@dataclass
class RunReport:
    run_id: str
    started_at: datetime
    status: str = "running"
    ingested: list[dict] = field(default_factory=list)
    silver: list[dict] = field(default_factory=list)
    gold_years: list[int] = field(default_factory=list)
    reconciliation: dict = field(default_factory=dict)
    warehouse: str | None = None
    error: str | None = None
    duration_s: float = 0.0


def landing_files(settings: Settings) -> list[Path]:
    return sorted(p for p in settings.landing.glob("*.csv") if LANDING_PATTERN.match(p.name))


# --------------------------------------------------------------------------------------------
# Etapas. Cada una decide qué le toca procesar comparando el estado de las tablas (manifiesto,
# batch publicado en silver, batch con el que se construyó gold). Así un run que falló a la
# mitad se retoma solo en la siguiente corrida, venga del CLI o de Dagster.
# --------------------------------------------------------------------------------------------
def stage_bronze(
    settings: Settings,
    files: list[Path] | None = None,
    years: list[int] | None = None,
    force: bool = False,
) -> list[bronze.IngestResult]:
    candidates = files or landing_files(settings)
    if years:
        candidates = [f for f in candidates if bronze.year_from_filename(f) in years]
    return [bronze.ingest_file(settings, f, force=force) for f in candidates]


def pending_silver(
    settings: Settings, years: list[int] | None = None, force: bool = False
) -> dict[int, str]:
    latest = bronze.latest_batch_per_year(settings)
    published = silver.published_batches(settings)
    pending = {y: b for y, b in latest.items() if published.get(y) != b}
    if force:
        pending.update({y: b for y, b in latest.items() if not years or y in years})
    if years:
        pending = {y: b for y, b in pending.items() if y in years}
    return pending


def stage_silver(
    settings: Settings, run_id: str, pending: dict[int, str]
) -> list[silver.SilverResult]:
    return [silver.build_year(settings, y, b, run_id) for y, b in sorted(pending.items())]


def stage_gold_dims(settings: Settings) -> None:
    gold.build_dim_tiempo(settings)
    for dim in gold.SCD1_DIMS:
        gold.build_scd1(settings, dim)
    gold.build_dim_entidad(settings)


def pending_gold(settings: Settings, force_years: list[int] | None = None) -> set[int]:
    published = silver.published_batches(settings)
    built = gold.built_batches(settings)
    years = {y for y, b in published.items() if built.get(y) != b}
    years |= _years_with_stale_keys(settings)
    years |= {y for y in (force_years or []) if y in published}
    return years


def stage_gold_facts(settings: Settings, years: set[int]) -> dict[str, dict]:
    out = {}
    for year in sorted(years):
        gold.build_fact_ejecucion(settings, year)
        gold.build_fact_presupuesto(settings, year)
        out[str(year)] = {k: str(v) for k, v in gold.reconcile(settings, year).items()}
    return out


def run(
    settings: Settings,
    files: list[Path] | None = None,
    years: list[int] | None = None,
    force: bool = False,
    build_warehouse: bool = True,
) -> RunReport:
    settings.ensure_dirs()
    report = RunReport(run_id=uuid.uuid4().hex, started_at=datetime.now(UTC))
    t0 = time.perf_counter()
    try:
        report.ingested = [asdict(r) for r in stage_bronze(settings, files, years, force)]

        pending = pending_silver(settings, years, force)
        report.silver = [asdict(r) for r in stage_silver(settings, report.run_id, pending)]

        gold_years = pending_gold(settings, force_years=years if force else None)
        if not pending and not gold_years:
            log.info("No hay datos nuevos: nada que procesar")
            report.status = "noop"
            return report

        stage_gold_dims(settings)
        gold_years |= _years_with_stale_keys(settings)
        report.reconciliation = stage_gold_facts(settings, gold_years)
        report.gold_years = sorted(gold_years)

        if build_warehouse:
            report.warehouse = str(export.build_warehouse(settings))
        report.status = "success"
        return report
    except Exception as exc:
        report.status = "failed"
        report.error = f"{type(exc).__name__}: {exc}"
        raise
    finally:
        report.duration_s = round(time.perf_counter() - t0, 2)
        _record(settings, report)
        log.info("run %s terminó en %s (%.1fs)", report.run_id, report.status, report.duration_s)


def _years_with_stale_keys(settings: Settings) -> set[int]:
    """Años de hechos que apuntan a versiones de entidad que ya no existen.

    Si se recarga un año pasado, la historia SCD2 puede partirse distinto y cambiar llaves
    de entidad usadas por otros años. Esos años se recalculan solos.
    """
    fact = settings.table("gold", "fact_ejecucion_mensual")
    if not storage.exists(fact):
        return set()
    dim = storage.scan(settings.table("gold", "dim_entidad")).select("entidad_sk")
    stale = (
        storage.scan(fact)
        .select("ano_eje", "entidad_sk")
        .unique()
        .join(dim, on="entidad_sk", how="anti")
        .select(pl.col("ano_eje").unique())
        .collect()
    )
    years = set(stale["ano_eje"].cast(pl.Int32).to_list())
    if years:
        log.info("Llaves de entidad desactualizadas en %s: se recalculan", sorted(years))
    return years


def _record(settings: Settings, report: RunReport) -> None:
    row = pl.DataFrame(
        {
            "run_id": [report.run_id],
            "started_at": [report.started_at],
            "status": [report.status],
            "duration_s": [report.duration_s],
            "years": [",".join(str(s["year"]) for s in report.silver)],
            "error": [report.error],
            "details": [json.dumps(asdict(report), default=str, ensure_ascii=False)],
        },
        schema_overrides={"error": pl.String},
    )
    try:
        storage.append(row, settings.table(*RUNS))
    except Exception:  # el registro nunca debe tapar el error original
        log.exception("No se pudo registrar la corrida %s", report.run_id)
