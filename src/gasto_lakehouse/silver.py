"""Capa silver: datos tipados, limpios, deduplicados y al grano declarado.

Flujo por año (un año = una partición):

1. Se normaliza y se evalúan las reglas de calidad una sola vez, en streaming, a un Parquet de
   trabajo temporal. Normalizar es lo caro; todo lo que sigue lee de ese archivo.
2. Métricas de calidad. Si las filas en error superan el umbral, se aborta aquí y no se publica
   nada (circuit breaker).
3. Las filas en error se guardan en ``quarantine/gasto`` con el motivo.
4. El año se procesa mes por mes (el mes es parte del grano, así que deduplicar por mes es
   exacto) y todos los meses se escriben en **una sola transacción** Delta que reemplaza la
   partición del año. Memoria acotada a un mes y lectores que nunca ven un año a medio escribir.
"""

from __future__ import annotations

import logging
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pyarrow as pa
from deltalake import write_deltalake

from gasto_lakehouse import bronze, quality, storage
from gasto_lakehouse.config import Settings
from gasto_lakehouse.schema import (
    AMOUNT_COLUMNS,
    CODE_COLUMNS,
    GRAIN,
    MONEY,
    NAME_COLUMNS,
    SILVER_SCHEMA,
)

log = logging.getLogger(__name__)

SILVER = ("silver", "gasto")
QUARANTINE = ("quarantine", "gasto")
QUALITY_RESULTS = ("_meta", "quality_results")


@dataclass(frozen=True)
class SilverResult:
    year: int
    batch_id: str
    rows_in: int
    rows_quarantined: int
    duplicates_removed: int
    rows_out: int
    error_ratio: float


def _clean_text(col: str) -> pl.Expr:
    return pl.col(col).str.strip_chars().str.replace_all(r"\s+", " ")


def _parse_amount(col: str) -> pl.Expr:
    raw = pl.col(col).str.strip_chars()
    # "1234,56" -> coma decimal; "1,234.56" -> coma de miles.
    normalized = (
        pl.when(raw.str.contains(r"^-?\d+,\d{1,2}$"))
        .then(raw.str.replace(",", "."))
        .otherwise(raw.str.replace_all(",", ""))
    )
    return normalized.cast(pl.Float64, strict=False).round(2).cast(MONEY, strict=False)


def normalize(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Limpieza y tipado. Conserva el texto crudo de los montos para la regla de calidad."""
    raw = [pl.col(c).str.strip_chars().alias(f"{c}__raw") for c in AMOUNT_COLUMNS]
    typed = [
        pl.col("ano_eje").str.strip_chars().cast(pl.Int16, strict=False),
        pl.col("mes_eje").str.strip_chars().cast(pl.Int8, strict=False),
        *(_clean_text(c) for c in CODE_COLUMNS),
        *(_clean_text(c).str.to_uppercase() for c in NAME_COLUMNS),
        *(_parse_amount(c).alias(c) for c in AMOUNT_COLUMNS),
    ]
    return lf.with_columns(raw).with_columns(typed)


def _to_grain(df: pl.DataFrame) -> tuple[pl.DataFrame, int]:
    """Quita duplicados exactos y consolida al grano (los montos son aditivos)."""
    before = df.height
    df = df.unique(subset=[*GRAIN, *NAME_COLUMNS, *AMOUNT_COLUMNS], maintain_order=False)
    dups = before - df.height
    out = df.group_by(GRAIN).agg(
        *(pl.col(c).max() for c in NAME_COLUMNS),
        *(pl.col(c).sum() for c in AMOUNT_COLUMNS),
    )
    return out.select(list(SILVER_SCHEMA)).cast(SILVER_SCHEMA), dups


def build_year(settings: Settings, year: int, batch_id: str, run_id: str) -> SilverResult:
    """Construye la partición ``ano_eje = year`` de silver a partir de un batch de bronze."""
    tmp = settings.root / "_tmp" / run_id / str(year)
    tmp.mkdir(parents=True, exist_ok=True)
    try:
        return _build_year(settings, year, batch_id, run_id, tmp)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def _build_year(
    settings: Settings, year: int, batch_id: str, run_id: str, tmp: Path
) -> SilverResult:
    # Normalizar es lo caro (regex sobre ~55 columnas de texto): se hace una sola vez, en
    # streaming, a un Parquet de trabajo. Todo lo demás lee de ahí.
    staged = tmp / "checked.parquet"
    quality.evaluate(normalize(bronze.scan_batch(settings, batch_id, year))).sink_parquet(staged)
    checked = pl.scan_parquet(staged)

    # --- 1. pasada de calidad -------------------------------------------------------------
    ok_cols = [c for c in checked.collect_schema().names() if c.startswith("_ok__")]
    stats = checked.select(
        pl.len().alias("_n"),
        pl.col("_es_valida").sum().alias("_validas"),
        *(pl.col(c).sum() for c in ok_cols),
    ).collect()
    total = int(stats["_n"][0])
    if total == 0:
        raise ValueError(f"El batch {batch_id} del año {year} no tiene filas")

    summary = _quality_summary(stats, ok_cols, total, run_id, batch_id, year)
    storage.append(summary, settings.table(*QUALITY_RESULTS))
    error_ratio = quality.gate(total - int(stats["_validas"][0]), total, settings.max_error_ratio)

    # --- 2. cuarentena -------------------------------------------------------------------
    bad = (
        checked.filter(~pl.col("_es_valida"))
        .select(
            "_source_year",
            "_batch_id",
            "_source_file",
            "_row_number",
            "_motivo_error",
            pl.lit(run_id).alias("_run_id"),
            *(pl.col(c).cast(pl.String) for c in (*GRAIN, *NAME_COLUMNS)),
            *(pl.col(f"{c}__raw").alias(c) for c in AMOUNT_COLUMNS),
        )
        .collect(engine="streaming")
    )
    if bad.height or storage.exists(settings.table(*QUARANTINE)):
        storage.overwrite(
            bad,
            settings.table(*QUARANTINE),
            partition_by=["_source_year"],
            replace_where=f"_source_year = {year}",
        )

    # --- 3. escritura mes a mes en una sola transacción ----------------------------------
    valid = checked.filter(pl.col("_es_valida"))
    months = (
        valid.select(pl.col("mes_eje").unique()).collect(engine="streaming")["mes_eje"].sort()
    ).to_list()

    counters = {"rows_out": 0, "dups": 0}
    updated_at = datetime.now(UTC)

    def month_batches() -> Iterator[pa.RecordBatch]:
        for m in months:
            df = valid.filter(pl.col("mes_eje") == m).collect()
            df, dups = _to_grain(df)
            df = df.with_columns(
                _batch_id=pl.lit(batch_id),
                _run_id=pl.lit(run_id),
                _updated_at=pl.lit(updated_at).dt.cast_time_unit("us"),
            )
            counters["rows_out"] += df.height
            counters["dups"] += dups
            log.info("silver %s-%02d: %s filas (%s duplicados)", year, m, df.height, dups)
            yield from df.to_arrow().to_batches()

    schema = _arrow_schema()
    target = settings.table(*SILVER)
    Path(target).mkdir(parents=True, exist_ok=True)
    write_deltalake(
        target,
        pa.RecordBatchReader.from_batches(schema, (b.cast(schema) for b in month_batches())),
        mode="overwrite",
        partition_by=["ano_eje"],
        predicate=f"ano_eje = {year}" if storage.exists(target) else None,
    )

    result = SilverResult(
        year=year,
        batch_id=batch_id,
        rows_in=total,
        rows_quarantined=bad.height,
        duplicates_removed=counters["dups"],
        rows_out=counters["rows_out"],
        error_ratio=error_ratio,
    )
    log.info("silver: %s", result)
    return result


def published_batches(settings: Settings) -> dict[int, str]:
    """Batch de bronze publicado hoy en silver para cada año."""
    path = settings.table(*SILVER)
    if not storage.exists(path):
        return {}
    df = storage.scan(path).group_by("ano_eje").agg(pl.col("_batch_id").max()).collect()
    return dict(zip(df["ano_eje"].cast(pl.Int32).to_list(), df["_batch_id"].to_list(), strict=True))


def _arrow_schema() -> pa.Schema:
    empty = pl.DataFrame(schema=SILVER_SCHEMA).with_columns(
        _batch_id=pl.lit("", pl.String),
        _run_id=pl.lit("", pl.String),
        _updated_at=pl.lit(datetime.now(UTC)).dt.cast_time_unit("us"),
    )
    return empty.to_arrow().schema


def _quality_summary(
    stats: pl.DataFrame, ok_cols: list[str], total: int, run_id: str, batch_id: str, year: int
) -> pl.DataFrame:
    now = datetime.now(UTC)
    by_name = {r.name: r for r in quality.rules()}
    rows = []
    for c in ok_cols:
        r = by_name[c.removeprefix("_ok__")]
        failed = total - int(stats[c][0])
        rows.append(
            {
                "run_id": run_id,
                "batch_id": batch_id,
                "ano_eje": year,
                "regla": r.name,
                "severidad": r.severity,
                "descripcion": r.description,
                "filas_evaluadas": total,
                "filas_fallidas": failed,
                "tasa_cumplimiento": round(1 - failed / total, 6),
                "evaluado_en": now,
            }
        )
    return pl.DataFrame(rows)
