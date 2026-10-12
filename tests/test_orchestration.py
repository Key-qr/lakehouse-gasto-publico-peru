import pytest

dagster = pytest.importorskip("dagster")

from gasto_lakehouse import orchestration as o  # noqa: E402
from gasto_lakehouse.ingestion.sample import SampleSpec, write_sample  # noqa: E402


def test_definiciones_cargan():
    defs = o.defs
    assert defs.resolve_job_def("pipeline_completo") is not None
    assert {s.name for s in defs.schedules} == {"pipeline_completo_schedule"}


def test_materializa_assets_y_pasan_los_checks(settings):
    write_sample(SampleSpec(year=2024, rows=2_400), settings.landing)
    res = dagster.materialize(
        [
            o.bronze_gasto,
            o.silver_gasto,
            o.gold_dimensiones,
            o.gold_hechos,
            o.warehouse_duckdb,
            o.calidad_silver,
            o.reconciliacion_gold,
        ],
        resources={"lakehouse": o.LakehouseResource(root=str(settings.root))},
        instance=dagster.DagsterInstance.ephemeral(),
    )
    assert res.success
    checks = {c.check_name: c.passed for c in res.get_asset_check_evaluations()}
    assert checks == {"calidad_silver": True, "reconciliacion_gold": True}
    assert settings.warehouse_db.exists()
