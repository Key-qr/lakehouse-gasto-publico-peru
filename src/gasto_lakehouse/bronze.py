"""Capa bronze: CSV crudo -> Delta, sin transformar valores.

Decisiones:
- Todo se guarda como texto. Bronze es la copia fiel de la fuente; tipar aquí haría perder las
  filas con valores raros que justamente queremos auditar en silver.
- Lectura en streaming con pyarrow (bloques de 16 MB): un archivo anual de varios GB se ingiere
  con memoria acotada, sin cargarlo entero.
- Se detecta separador y encoding (el MEF ha publicado archivos en UTF-8 y en Latin-1).
- Un manifiesto con el SHA-256 de cada archivo evita reingerir lo mismo dos veces.
"""

from __future__ import annotations

import csv
import hashlib
import logging
import re
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.csv as pacsv
from deltalake import write_deltalake

from gasto_lakehouse import storage
from gasto_lakehouse.config import Settings
from gasto_lakehouse.schema import COLUMNS, snake

log = logging.getLogger(__name__)

BRONZE = ("bronze", "gasto")
MANIFEST = ("_meta", "ingestion_manifest")


@dataclass(frozen=True)
class IngestResult:
    batch_id: str
    source_file: str
    source_year: int
    rows: int
    sha256: str
    skipped: bool = False


def sha256_file(path: Path, chunk: int = 8 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(chunk), b""):
            h.update(block)
    return h.hexdigest()


def sniff(path: Path, sample_bytes: int = 1 << 20) -> tuple[str, str]:
    """Devuelve (delimitador, encoding) mirando el primer MB del archivo."""
    with open(path, "rb") as fh:
        raw = fh.read(sample_bytes)
    try:
        text = raw.decode("utf-8")
        encoding = "utf-8"
    except UnicodeDecodeError as exc:
        # Si el corte cayó a mitad de un carácter multibyte no es un error real.
        if exc.start >= len(raw) - 4:
            text, encoding = raw[: exc.start].decode("utf-8"), "utf-8"
        else:
            text, encoding = raw.decode("cp1252", errors="replace"), "cp1252"
    header = text.splitlines()[0] if text else ""
    try:
        delimiter = csv.Sniffer().sniff(header, delimiters=",;|\t").delimiter
    except csv.Error:
        delimiter = ","
    return delimiter, encoding


def year_from_filename(path: Path) -> int:
    m = re.match(r"(\d{4})", Path(path).name)
    if not m:
        raise ValueError(f"No se pudo inferir el año desde el nombre del archivo: {path.name}")
    return int(m.group(1))


def _batches(
    path: Path, delimiter: str, encoding: str, block_size: int, meta: dict
) -> tuple[pa.Schema, Iterator[pa.RecordBatch]]:
    # Primera pasada solo para leer el header y forzar todo a string.
    with open(path, encoding=encoding, errors="replace") as fh:
        header = next(csv.reader(fh, delimiter=delimiter))
    cols = [snake(h).lstrip("﻿") for h in header]
    missing = set(COLUMNS) - set(cols)
    if missing:
        raise ValueError(f"{path.name}: faltan columnas del contrato: {sorted(missing)}")

    reader = pacsv.open_csv(
        path,
        read_options=pacsv.ReadOptions(
            encoding=encoding, block_size=block_size, column_names=cols, skip_rows=1
        ),
        parse_options=pacsv.ParseOptions(delimiter=delimiter, newlines_in_values=False),
        convert_options=pacsv.ConvertOptions(
            column_types={c: pa.string() for c in cols},
            include_columns=list(COLUMNS),
            strings_can_be_null=False,
        ),
    )
    schema = pa.schema(
        [pa.field(c, pa.string()) for c in COLUMNS]
        + [
            pa.field("_batch_id", pa.string()),
            pa.field("_source_file", pa.string()),
            pa.field("_source_year", pa.int32()),
            pa.field("_ingested_at", pa.timestamp("us", tz="UTC")),
            pa.field("_row_number", pa.int64()),
        ]
    )

    def gen() -> Iterator[pa.RecordBatch]:
        offset = 0
        for batch in reader:
            n = batch.num_rows
            arrays = [batch.column(c) for c in COLUMNS] + [
                pa.array([meta["_batch_id"]] * n, pa.string()),
                pa.array([meta["_source_file"]] * n, pa.string()),
                pa.array([meta["_source_year"]] * n, pa.int32()),
                pa.array([meta["_ingested_at"]] * n, pa.timestamp("us", tz="UTC")),
                pa.array(range(offset + 1, offset + n + 1), pa.int64()),
            ]
            offset += n
            yield pa.RecordBatch.from_arrays(arrays, schema=schema)

    return schema, gen()


def already_ingested(settings: Settings, sha: str) -> bool:
    path = settings.table(*MANIFEST)
    if not storage.exists(path):
        return False
    return storage.scan(path).filter(pl.col("sha256") == sha).select(pl.len()).collect().item() > 0


def ingest_file(settings: Settings, path: Path, force: bool = False) -> IngestResult:
    path = Path(path)
    sha = sha256_file(path)
    year = year_from_filename(path)
    if not force and already_ingested(settings, sha):
        log.info("bronze: %s ya fue ingerido (sha256=%s…), se omite", path.name, sha[:12])
        return IngestResult("", path.name, year, 0, sha, skipped=True)

    delimiter, encoding = sniff(path)
    batch_id = uuid.uuid4().hex
    meta = {
        "_batch_id": batch_id,
        "_source_file": path.name,
        "_source_year": year,
        "_ingested_at": datetime.now(UTC),
    }
    log.info("bronze: ingiriendo %s (delim=%r, encoding=%s)", path.name, delimiter, encoding)
    schema, batches = _batches(path, delimiter, encoding, settings.csv_block_size, meta)

    rows = 0

    def counted() -> Iterator[pa.RecordBatch]:
        nonlocal rows
        for b in batches:
            rows += b.num_rows
            yield b

    target = settings.table(*BRONZE)
    Path(target).mkdir(parents=True, exist_ok=True)
    write_deltalake(
        target,
        pa.RecordBatchReader.from_batches(schema, counted()),
        mode="append",
        partition_by=["_source_year"],
    )

    manifest = pl.DataFrame(
        {
            "source_file": [path.name],
            "sha256": [sha],
            "size_bytes": [path.stat().st_size],
            "source_year": [year],
            "rows": [rows],
            "batch_id": [batch_id],
            "delimiter": [delimiter],
            "encoding": [encoding],
            "ingested_at": [meta["_ingested_at"]],
        }
    )
    storage.merge(manifest, settings.table(*MANIFEST), keys=["source_file", "sha256"])
    log.info("bronze: %s filas de %s -> batch %s", rows, path.name, batch_id)
    return IngestResult(batch_id, path.name, year, rows, sha)


def scan_batch(settings: Settings, batch_id: str, year: int) -> pl.LazyFrame:
    return storage.scan(settings.table(*BRONZE)).filter(
        (pl.col("_source_year") == year) & (pl.col("_batch_id") == batch_id)
    )


def latest_batch_per_year(settings: Settings) -> dict[int, str]:
    """Último batch ingerido por año, según el manifiesto."""
    path = settings.table(*MANIFEST)
    if not storage.exists(path):
        return {}
    df = (
        storage.read(path)
        .sort("ingested_at")
        .group_by("source_year")
        .agg(pl.col("batch_id").last())
    )
    return dict(zip(df["source_year"].to_list(), df["batch_id"].to_list(), strict=True))
