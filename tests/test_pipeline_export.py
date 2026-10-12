import duckdb
import polars as pl
import pytest

from gasto_lakehouse import export, pipeline, quality, storage
from gasto_lakehouse.ingestion.sample import SampleSpec, generate, write_sample


def test_segunda_corrida_es_noop(built):
    before = {t: storage.version(built.table("gold", t)) for t in export.GOLD_TABLES}
    report = pipeline.run(built)
    assert report.status == "noop"
    assert {t: storage.version(built.table("gold", t)) for t in export.GOLD_TABLES} == before


def test_corridas_quedan_registradas(built):
    runs = storage.read(built.table(*pipeline.RUNS))
    assert {"success"} <= set(runs["status"])


def test_warehouse_tiene_gold_y_marts(built):
    con = duckdb.connect(str(built.warehouse_db), read_only=True)
    tables = {
        r[0]
        for r in con.execute(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables"
        ).fetchall()
    }
    for t in export.GOLD_TABLES:
        assert f"gold.{t}" in tables
    for m in export.MART_TABLES:
        assert f"marts.{m}" in tables
        assert con.execute(f"SELECT count(*) FROM marts.{m}").fetchone()[0] > 0

    # Los marts cuadran con el hecho.
    mart, fact = con.execute(
        "SELECT (SELECT sum(devengado) FROM marts.avance_departamento WHERE ano_eje = 2023),"
        "       (SELECT sum(monto_devengado) FROM gold.fact_presupuesto_anual WHERE ano_eje = 2023)"
    ).fetchone()
    assert mart == fact

    # La curva acumulada no retrocede si no hay reversiones netas.
    curva = con.execute(
        "SELECT devengado_acumulado FROM marts.curva_ejecucion WHERE ano = 2023 ORDER BY mes"
    ).fetchall()
    values = [v[0] for v in curva]
    assert values == sorted(values)
    con.close()


def test_export_de_marts_para_la_demo(built, tmp_path):
    paths = export.export_marts(built.warehouse_db, tmp_path)
    assert len(paths) == len(export.MART_TABLES)
    assert pl.read_parquet(paths[0]).height > 0


def test_falla_de_calidad_queda_registrada_y_silver_intacto(settings):
    write_sample(SampleSpec(year=2023, rows=1_200), settings.landing)
    assert pipeline.run(settings).status == "success"
    silver_v = storage.version(settings.table("silver", "gasto"))

    bad = settings.landing / "2024-Gasto.csv"
    generate(SampleSpec(year=2024, rows=600, dirty=False)).with_columns(
        MES_EJE=pl.lit("99")
    ).write_csv(bad)
    with pytest.raises(quality.QualityGateError):
        pipeline.run(settings)

    assert storage.version(settings.table("silver", "gasto")) == silver_v
    runs = storage.read(settings.table(*pipeline.RUNS)).sort("started_at")
    assert runs["status"][-1] == "failed"
    assert "QualityGateError" in runs["error"][-1]


def test_recarga_de_un_anio_pasado_recalcula_llaves_de_otros(settings):
    """Recargar 2023 con otra historia cambia las versiones SCD2 que usa 2024."""
    write_sample(SampleSpec(year=2023, rows=1_200, dirty=False), settings.landing)
    write_sample(SampleSpec(year=2024, rows=1_200), settings.landing)
    assert pipeline.run(settings).status == "success"

    write_sample(SampleSpec(year=2023, rows=1_200), settings.landing)  # ahora con renombres
    report = pipeline.run(settings)
    assert report.status == "success"
    assert report.gold_years == [2023, 2024]
    for y in (2023, 2024):
        assert report.reconciliation[str(y)]["huerfanos"] == "0"
