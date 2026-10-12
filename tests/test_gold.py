import polars as pl
import pytest

from gasto_lakehouse import gold, storage
from gasto_lakehouse.ingestion.sample import RELOCATE_FROM, RENAME_FROM
from gasto_lakehouse.schema import CODE_COLUMNS, SILVER_SCHEMA
from gasto_lakehouse.silver import SILVER


def test_surrogate_key_es_determinista():
    assert gold.surrogate_key("300000", "202301") == gold.surrogate_key("300000", "202301")
    assert gold.surrogate_key("300000", "202301") != gold.surrogate_key("300000", "202302")
    assert 0 < gold.surrogate_key("x") < 2**63


def test_scd2_abre_version_en_cada_cambio(built):
    dim = storage.read(built.table("gold", "dim_entidad"))
    multi = dim.filter(pl.col("sec_ejec").is_duplicated()).sort("sec_ejec", "valido_desde")

    renamed = multi.filter(pl.col("ejecutora_nombre").str.ends_with("(REORGANIZADA)"))
    assert renamed.height == 2
    assert set(renamed["valido_desde"]) == {RENAME_FROM}

    moved = multi.filter(pl.col("provincia_ejecutora") == "02")
    assert moved["valido_desde"].to_list() == [RELOCATE_FROM]


def test_scd2_rangos_sin_huecos_ni_solapes(built):
    dim = storage.read(built.table("gold", "dim_entidad")).sort("sec_ejec", "valido_desde")
    assert (
        dim.group_by("sec_ejec").agg(pl.col("es_vigente").sum())["es_vigente"].to_list()
        == [1] * dim["sec_ejec"].n_unique()
    )
    chained = dim.with_columns(prev_hasta=pl.col("valido_hasta").shift(1).over("sec_ejec")).filter(
        pl.col("prev_hasta").is_not_null()
    )
    assert (chained["prev_hasta"] == chained["valido_desde"]).all()
    assert (dim["valido_desde"] < dim["valido_hasta"]).all()


def test_hechos_apuntan_a_la_version_vigente_en_el_periodo(built):
    dim = storage.read(built.table("gold", "dim_entidad"))
    fact = storage.read(built.table("gold", "fact_ejecucion_mensual"))
    joined = fact.join(dim, on="entidad_sk")
    assert joined.height == fact.height
    assert (
        (joined["periodo_sk"] >= joined["valido_desde"])
        & (joined["periodo_sk"] < joined["valido_hasta"])
    ).all()


def test_reconciliacion_al_centimo(built):
    for year in (2023, 2024):
        r = gold.reconcile(built, year)
        assert r["silver"] == r["fact_ejecucion"] == r["fact_presupuesto"]
        assert r["huerfanos"] == 0


def test_presupuesto_stock_no_se_multiplica_por_meses(built):
    """El PIM viene repetido cada mes: el hecho anual toma el último mes de cada línea."""
    silver = storage.scan(built.table(*SILVER)).filter(pl.col("ano_eje") == 2023)
    per_line = (
        silver.sort("mes_eje")
        .group_by(CODE_COLUMNS)
        .agg(ultimo=pl.col("monto_pim").last(), suma=pl.col("monto_pim").sum())
    )
    expected, naive = per_line.select(pl.col("ultimo").sum(), pl.col("suma").sum()).collect().row(0)
    fact_pim = (
        storage.scan(built.table("gold", "fact_presupuesto_anual"))
        .filter(pl.col("ano_eje") == 2023)
        .select(pl.col("monto_pim").sum())
        .collect()
        .item()
    )
    assert fact_pim == expected
    assert naive > 10 * fact_pim  # sumar meses habría inflado el presupuesto ~12 veces


@pytest.mark.parametrize(("pim_months", "expected"), [((1, 2, 3), "latest"), ((1,), "sum")])
def test_deteccion_de_semantica_del_presupuesto(settings, pim_months, expected):
    rows = []
    for line in range(20):
        for mes in (1, 2, 3):
            row = dict.fromkeys(SILVER_SCHEMA)
            row.update({c: "1" for c, t in SILVER_SCHEMA.items() if t == pl.String()})
            row.update(
                ano_eje=2024,
                mes_eje=mes,
                sec_func=str(line),
                monto_pim=100 if mes in pim_months else 0,
            )
            rows.append(row)
    df = pl.DataFrame(rows).cast(SILVER_SCHEMA).with_columns(_batch_id=pl.lit("b"))
    storage.overwrite(df, settings.table(*SILVER), partition_by=["ano_eje"])
    assert gold.budget_semantics(settings, 2024) == expected
