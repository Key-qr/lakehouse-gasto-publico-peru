import uuid

import polars as pl
import pytest

from gasto_lakehouse import bronze, quality, silver, storage
from gasto_lakehouse.ingestion.sample import SampleSpec, generate, write_sample
from gasto_lakehouse.schema import GRAIN


def _bronze_like(df: pl.DataFrame, year: int = 2024) -> pl.LazyFrame:
    return df.rename(str.lower).with_columns(_source_year=pl.lit(year, pl.Int32)).lazy()


def test_parseo_de_montos():
    df = pl.DataFrame({"x": ["1,234.50", "1234,5", " 10 ", "N/D", "", "-3.456"]})
    out = df.select(silver._parse_amount("x").cast(pl.Float64))["x"].to_list()
    assert out == [1234.5, 1234.5, 10.0, None, None, -3.46]


def test_normalizacion_de_nombres():
    base = generate(SampleSpec(year=2024, rows=12, dirty=False)).head(1)
    base = base.with_columns(EJECUTORA_NOMBRE=pl.lit("  municipalidad   de  trujillo "))
    out = silver.normalize(_bronze_like(base)).collect()
    assert out["ejecutora_nombre"][0] == "MUNICIPALIDAD DE TRUJILLO"


def test_cada_defecto_cae_en_su_regla():
    base = generate(SampleSpec(year=2024, rows=12, dirty=False)).head(5)
    bad = base.with_columns(
        NIVEL_GOBIERNO=pl.Series(["E", "X", "E", "E", "E"]),
        MES_EJE=pl.Series(["1", "1", "13", "1", "1"]),
        MONTO_PIM=pl.Series(["1", "1", "1", "abc", "1"]),
        ANO_EJE=pl.Series(["2024", "2024", "2024", "2024", "2023"]),
    )
    out = quality.evaluate(silver.normalize(_bronze_like(bad))).collect()
    assert out["_motivo_error"].to_list() == [
        "",
        "nivel_gobierno_valido",
        "mes_en_rango",
        "montos_numericos",
        "ano_coincide_con_archivo",
    ]
    assert out["_es_valida"].to_list() == [True, False, False, False, False]


def test_gate_corta_por_encima_del_umbral():
    assert quality.gate(5, 100, 0.05) == 0.05
    with pytest.raises(quality.QualityGateError):
        quality.gate(6, 100, 0.05)


def test_silver_deduplica_y_pone_en_cuarentena(settings):
    p = write_sample(SampleSpec(year=2024, rows=6_000), settings.landing)
    b = bronze.ingest_file(settings, p)
    res = silver.build_year(settings, 2024, b.batch_id, uuid.uuid4().hex)

    sv = storage.read(settings.table(*silver.SILVER))
    q = storage.read(settings.table(*silver.QUARANTINE))
    assert res.rows_quarantined == q.height > 0
    assert res.duplicates_removed > 0
    assert res.rows_out == sv.height
    assert res.rows_in == res.rows_out + res.rows_quarantined + res.duplicates_removed
    # El grano es único después de silver.
    assert sv.select(GRAIN).is_duplicated().sum() == 0
    # Las métricas de calidad quedan registradas.
    qr = storage.read(settings.table(*silver.QUALITY_RESULTS))
    assert set(qr["regla"]) == {r.name for r in quality.rules()}


def test_silver_rechaza_lote_y_no_publica_nada(settings):
    p = settings.landing / "2024-Gasto.csv"
    generate(SampleSpec(year=2024, rows=600, dirty=False)).with_columns(
        NIVEL_GOBIERNO=pl.lit("X")
    ).write_csv(p)
    b = bronze.ingest_file(settings, p)
    with pytest.raises(quality.QualityGateError):
        silver.build_year(settings, 2024, b.batch_id, "run")
    assert not storage.exists(settings.table(*silver.SILVER))


def test_recarga_del_anio_reemplaza_la_particion(settings):
    for rows in (3_000, 1_200):
        p = write_sample(SampleSpec(year=2024, rows=rows, seed=rows), settings.landing)
        b = bronze.ingest_file(settings, p)
        silver.build_year(settings, 2024, b.batch_id, "run")
    sv = storage.read(settings.table(*silver.SILVER))
    assert sv["_batch_id"].n_unique() == 1
    assert sv.height < 1_500
