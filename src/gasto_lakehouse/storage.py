"""Acceso a tablas Delta.

Encapsula delta-rs para que el resto del código no dependa de su API directamente. Si mañana
el storage pasa a S3/ADLS, solo cambia este módulo (ver docs/adr/0002-motor-y-almacenamiento.md).
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from pathlib import Path

import polars as pl
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

log = logging.getLogger(__name__)


def exists(path: str) -> bool:
    return DeltaTable.is_deltatable(path)


def scan(path: str, version: int | None = None) -> pl.LazyFrame:
    return pl.scan_delta(path, version=version)


def read(path: str, version: int | None = None) -> pl.DataFrame:
    return pl.read_delta(path, version=version)


def overwrite(
    df: pl.DataFrame,
    path: str,
    partition_by: Sequence[str] | None = None,
    replace_where: str | None = None,
) -> None:
    """Sobrescribe la tabla completa o solo las filas que cumplen ``replace_where``.

    ``replace_where`` es el equivalente a ``replaceWhere`` de Spark: reemplaza atómicamente una
    partición (p.ej. ``ano_eje = 2024``) sin tocar el resto. Es idempotente.
    """
    Path(path).mkdir(parents=True, exist_ok=True)
    opts: dict = {}
    if partition_by:
        opts["partition_by"] = list(partition_by)
    if replace_where and exists(path):
        opts["predicate"] = replace_where
    elif exists(path):
        # Reconstrucción completa: el esquema nuevo manda (las tablas gold se recalculan).
        opts["schema_mode"] = "overwrite"
    df.write_delta(path, mode="overwrite", delta_write_options=opts)
    log.info("overwrite %s rows=%s replace_where=%s", path, df.height, replace_where)


def append(df: pl.DataFrame, path: str, partition_by: Sequence[str] | None = None) -> None:
    Path(path).mkdir(parents=True, exist_ok=True)
    opts = {"partition_by": list(partition_by)} if partition_by else {}
    df.write_delta(path, mode="append", delta_write_options=opts)


def merge(
    df: pl.DataFrame,
    path: str,
    keys: Sequence[str],
    partition_by: Sequence[str] | None = None,
    update: bool = True,
) -> dict:
    """Upsert por llave natural. Crea la tabla si no existe."""
    if not exists(path):
        overwrite(df, path, partition_by=partition_by)
        return {"num_target_rows_inserted": df.height, "created": True}
    predicate = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    builder = df.write_delta(
        path,
        mode="merge",
        delta_merge_options={"predicate": predicate, "source_alias": "s", "target_alias": "t"},
    )
    if update:
        builder = builder.when_matched_update_all()
    metrics = builder.when_not_matched_insert_all().execute()
    log.info("merge %s %s", path, metrics)
    return metrics


def history(path: str, limit: int | None = None) -> list[dict]:
    try:
        return DeltaTable(path).history(limit)
    except TableNotFoundError:
        return []


def version(path: str) -> int | None:
    return DeltaTable(path).version() if exists(path) else None


def optimize(path: str, zorder_by: Sequence[str] | None = None) -> dict:
    dt = DeltaTable(path)
    if zorder_by:
        return dt.optimize.z_order(list(zorder_by))
    return dt.optimize.compact()


def vacuum(path: str, retention_hours: int = 168, dry_run: bool = True) -> list[str]:
    dt = DeltaTable(path)
    return dt.vacuum(
        retention_hours=retention_hours,
        dry_run=dry_run,
        enforce_retention_duration=retention_hours >= 168,
    )
