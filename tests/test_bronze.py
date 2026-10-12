from pathlib import Path

import polars as pl
import pytest

from gasto_lakehouse import bronze, storage
from gasto_lakehouse.ingestion.sample import SampleSpec, generate, write_sample
from gasto_lakehouse.schema import COLUMNS


def test_year_from_filename():
    assert bronze.year_from_filename(Path("2024-Gasto.csv")) == 2024
    assert bronze.year_from_filename(Path("2026-Gasto-Mensual.csv")) == 2026
    with pytest.raises(ValueError):
        bronze.year_from_filename(Path("Gasto.csv"))


def test_sniff_detecta_punto_y_coma_y_latin1(tmp_path):
    p = tmp_path / "2024-Gasto.csv"
    generate(SampleSpec(year=2024, rows=120, dirty=False)).write_csv(p, separator=";")
    p.write_bytes(p.read_text(encoding="utf-8").encode("cp1252"))
    assert bronze.sniff(p) == (";", "cp1252")


def test_ingesta_guarda_todo_como_texto_con_metadatos(settings):
    p = write_sample(SampleSpec(year=2024, rows=600), settings.landing, encoding="cp1252")
    res = bronze.ingest_file(settings, p)

    df = storage.read(settings.table(*bronze.BRONZE))
    assert res.rows == df.height > 0
    assert all(df.schema[c] == pl.String for c in COLUMNS)
    assert df["_row_number"].min() == 1
    assert df["_row_number"].max() == res.rows
    assert df["_batch_id"].unique().to_list() == [res.batch_id]
    # El "Nº" sobrevive a la conversión desde cp1252.
    assert df["meta_nombre"].str.contains("Nº").any()


def test_reingesta_del_mismo_archivo_se_omite(settings):
    p = write_sample(SampleSpec(year=2024, rows=300), settings.landing)
    first = bronze.ingest_file(settings, p)
    second = bronze.ingest_file(settings, p)
    assert not first.skipped and second.skipped
    assert storage.read(settings.table(*bronze.BRONZE)).height == first.rows


def test_archivo_sin_columnas_del_contrato_falla(settings):
    p = settings.landing / "2024-Gasto.csv"
    generate(SampleSpec(year=2024, rows=50)).drop("MONTO_PIM").write_csv(p)
    with pytest.raises(ValueError, match="monto_pim"):
        bronze.ingest_file(settings, p)
