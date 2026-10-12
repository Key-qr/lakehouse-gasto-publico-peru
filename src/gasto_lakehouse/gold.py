"""Capa gold: modelo dimensional (Kimball) sobre silver.

Dimensiones
- dim_tiempo                periodo_sk = AAAAMM
- dim_entidad               SCD tipo 2 (historia de nombre, sector, pliego y ubicación)
- dim_funcional             SCD tipo 1 (programa presupuestal + función/división/grupo)
- dim_clasificador_gasto    SCD tipo 1 (categoría + jerarquía genérica -> específica detalle)
- dim_fuente                SCD tipo 1 (fuente de financiamiento, rubro, tipo de recurso)
- dim_ubigeo                SCD tipo 1 (departamento donde se ejecuta la meta)
- dim_producto_proyecto     SCD tipo 1 (producto o proyecto de inversión)

Hechos
- fact_ejecucion_mensual    flujo mensual: certificado, comprometido, devengado, girado
- fact_presupuesto_anual    PIA/PIM (stock) + ejecución acumulada del año

Las llaves sustitutas son hashes deterministas de la llave natural (ver ADR 0004): el mismo
dato produce la misma llave en cualquier corrida, sin secuencias ni coordinación.
"""

from __future__ import annotations

import hashlib
import logging
from dataclasses import dataclass

import polars as pl

from gasto_lakehouse import storage
from gasto_lakehouse.config import Settings
from gasto_lakehouse.schema import BUDGET_COLUMNS, CODE_COLUMNS, EXECUTION_COLUMNS
from gasto_lakehouse.silver import SILVER

log = logging.getLogger(__name__)

OPEN_END = 999912


@dataclass(frozen=True)
class Dim:
    name: str
    natural_key: tuple[str, ...]
    attributes: tuple[str, ...]

    @property
    def sk(self) -> str:
        return self.name.removeprefix("dim_") + "_sk"


SCD1_DIMS: tuple[Dim, ...] = (
    Dim(
        "dim_funcional",
        ("programa_ppto", "funcion", "division_funcional", "grupo_funcional"),
        (
            "programa_ppto_nombre",
            "funcion_nombre",
            "division_funcional_nombre",
            "grupo_funcional_nombre",
        ),
    ),
    Dim(
        "dim_clasificador_gasto",
        (
            "tipo_transaccion",
            "categoria_gasto",
            "generica",
            "subgenerica",
            "subgenerica_det",
            "especifica",
            "especifica_det",
        ),
        (
            "categoria_gasto_nombre",
            "generica_nombre",
            "subgenerica_nombre",
            "subgenerica_det_nombre",
            "especifica_nombre",
            "especifica_det_nombre",
        ),
    ),
    Dim(
        "dim_fuente",
        ("fuente_financiamiento", "rubro", "tipo_recurso"),
        ("fuente_financiamiento_nombre", "rubro_nombre", "tipo_recurso_nombre"),
    ),
    Dim("dim_ubigeo", ("departamento_meta",), ("departamento_meta_nombre",)),
    Dim(
        "dim_producto_proyecto",
        ("tipo_act_proy", "producto_proyecto"),
        ("tipo_act_proy_nombre", "producto_proyecto_nombre"),
    ),
)

ENTIDAD = Dim(
    "dim_entidad",
    ("sec_ejec",),
    (
        "ejecutora",
        "ejecutora_nombre",
        "nivel_gobierno",
        "nivel_gobierno_nombre",
        "sector",
        "sector_nombre",
        "pliego",
        "pliego_nombre",
        "departamento_ejecutora",
        "departamento_ejecutora_nombre",
        "provincia_ejecutora",
        "provincia_ejecutora_nombre",
        "distrito_ejecutora",
        "distrito_ejecutora_nombre",
    ),
)

MESES = [
    "APERTURA",
    "ENERO",
    "FEBRERO",
    "MARZO",
    "ABRIL",
    "MAYO",
    "JUNIO",
    "JULIO",
    "AGOSTO",
    "SETIEMBRE",
    "OCTUBRE",
    "NOVIEMBRE",
    "DICIEMBRE",
]


def surrogate_key(*parts: str) -> int:
    """Hash estable (md5) de la llave natural -> int64 positivo."""
    digest = hashlib.md5("|".join(parts).encode("utf-8"), usedforsecurity=False).hexdigest()
    return int(digest[:15], 16)


def _with_sk(df: pl.DataFrame, cols: tuple[str, ...], sk: str) -> pl.DataFrame:
    keys = df.select(pl.concat_str([pl.col(c).fill_null("") for c in cols], separator="|"))
    values = [surrogate_key(k) for k in keys.to_series().to_list()]
    return df.with_columns(pl.Series(sk, values, dtype=pl.Int64))


def _silver(settings: Settings) -> pl.LazyFrame:
    return storage.scan(settings.table(*SILVER)).with_columns(
        periodo_sk=(pl.col("ano_eje").cast(pl.Int32) * 100 + pl.col("mes_eje").cast(pl.Int32))
    )


# --------------------------------------------------------------------------------------------
# Dimensiones
# --------------------------------------------------------------------------------------------
def build_dim_tiempo(settings: Settings) -> pl.DataFrame:
    df = (
        _silver(settings)
        .select("periodo_sk", "ano_eje", "mes_eje")
        .unique()
        .collect()
        .sort("periodo_sk")
        .with_columns(
            ano=pl.col("ano_eje").cast(pl.Int16),
            mes=pl.col("mes_eje").cast(pl.Int8),
            nombre_mes=pl.col("mes_eje").map_elements(lambda m: MESES[m], return_dtype=pl.String),
            trimestre=((pl.col("mes_eje").cast(pl.Int8).clip(1, 12) - 1) // 3 + 1).cast(pl.Int8),
            semestre=((pl.col("mes_eje").cast(pl.Int8).clip(1, 12) - 1) // 6 + 1).cast(pl.Int8),
        )
        .select("periodo_sk", "ano", "mes", "nombre_mes", "trimestre", "semestre")
    )
    storage.overwrite(df, settings.table("gold", "dim_tiempo"))
    return df


def build_scd1(settings: Settings, dim: Dim) -> dict:
    """Upsert SCD1: el nombre vigente es el del periodo más reciente."""
    df = (
        _silver(settings)
        .select("periodo_sk", *dim.natural_key, *dim.attributes)
        .sort("periodo_sk")
        .group_by(dim.natural_key)
        .agg(
            *(pl.col(a).last() for a in dim.attributes),
            pl.col("periodo_sk").max().alias("ultimo_periodo"),
        )
        .collect()
    )
    df = _with_sk(df, dim.natural_key, dim.sk).select(
        dim.sk, *dim.natural_key, *dim.attributes, "ultimo_periodo"
    )
    return storage.merge(df, settings.table("gold", dim.name), keys=[dim.sk])


def build_dim_entidad(settings: Settings) -> pl.DataFrame:
    """SCD tipo 2 por "gaps and islands".

    Se arma una foto por (entidad, periodo) y se abre una versión nueva cada vez que cambia
    algún atributo respecto al periodo anterior. Se reconstruye completa a partir de silver:
    es determinista, soporta recargas de años pasados en cualquier orden y la dimensión es
    chica (miles de filas). Detalle en docs/adr/0005-scd2-entidad.md.
    """
    attrs = list(ENTIDAD.attributes)
    snap = (
        _silver(settings)
        .group_by("sec_ejec", "periodo_sk")
        .agg(*(pl.col(a).max() for a in attrs))
        .collect()
        .sort("sec_ejec", "periodo_sk")
        .with_columns(_firma=pl.concat_str([pl.col(a).fill_null("") for a in attrs], separator="|"))
        .with_columns(
            _cambio=(pl.col("_firma") != pl.col("_firma").shift(1).over("sec_ejec")).fill_null(True)
        )
        .with_columns(version=pl.col("_cambio").cast(pl.Int32).cum_sum().over("sec_ejec"))
    )
    dim = (
        snap.group_by("sec_ejec", "version", maintain_order=True)
        .agg(pl.col("periodo_sk").min().alias("valido_desde"), *(pl.col(a).first() for a in attrs))
        .sort("sec_ejec", "valido_desde")
        .with_columns(
            valido_hasta=pl.col("valido_desde").shift(-1).over("sec_ejec").fill_null(OPEN_END),
        )
        .with_columns(es_vigente=pl.col("valido_hasta") == OPEN_END)
    )
    dim = dim.with_columns(
        _k=pl.concat_str(
            [pl.col("sec_ejec"), pl.col("valido_desde").cast(pl.String)], separator="|"
        )
    )
    dim = _with_sk(dim, ("_k",), "entidad_sk").select(
        "entidad_sk", "sec_ejec", "version", "valido_desde", "valido_hasta", "es_vigente", *attrs
    )
    storage.overwrite(dim, settings.table("gold", "dim_entidad"))
    log.info("dim_entidad: %s versiones de %s entidades", dim.height, dim["sec_ejec"].n_unique())
    return dim


# --------------------------------------------------------------------------------------------
# Hechos
# --------------------------------------------------------------------------------------------
def budget_semantics(settings: Settings, year: int) -> str:
    """Decide cómo agregar PIA/PIM en el año.

    Si en las líneas con varios meses el PIM aparece en casi todos los meses, el archivo lo
    trae como stock (foto acumulada) y hay que tomar el último mes; sumarlo multiplicaría el
    presupuesto. Si aparece en un solo mes por línea, se suma. Se puede forzar con
    LAKEHOUSE_BUDGET_SEMANTICS=latest|sum.
    """
    if settings.budget_semantics in ("latest", "sum"):
        return settings.budget_semantics
    keys = [c for c in CODE_COLUMNS]
    prof = (
        _silver(settings)
        .filter(pl.col("ano_eje") == year)
        .group_by(keys)
        .agg(meses=pl.len(), con_pim=(pl.col("monto_pim") != 0).sum())
        .filter(pl.col("meses") > 1)
        .select(pl.col("con_pim").sum() / pl.col("meses").sum())
        .collect(engine="streaming")
        .item()
    )
    mode = "latest" if prof is None or prof >= 0.8 else "sum"
    log.info("presupuesto %s: %.1f%% de meses con PIM -> modo %s", year, (prof or 0) * 100, mode)
    return mode


def _attach_sks(lf: pl.LazyFrame, settings: Settings) -> pl.LazyFrame:
    for dim in SCD1_DIMS:
        d = storage.scan(settings.table("gold", dim.name)).select(dim.sk, *dim.natural_key)
        lf = lf.join(d, on=list(dim.natural_key), how="left", nulls_equal=True)
    return lf


def _entity_asof(df: pl.DataFrame, settings: Settings, period_col: str) -> pl.DataFrame:
    ent = (
        storage.read(settings.table("gold", "dim_entidad"))
        .select("sec_ejec", "valido_desde", "entidad_sk")
        .sort("valido_desde")
    )
    return (
        df.sort(period_col)
        .join_asof(
            ent,
            left_on=period_col,
            right_on="valido_desde",
            by="sec_ejec",
            strategy="backward",
            check_sortedness=False,  # ambos lados vienen ordenados por periodo arriba
        )
        .drop("valido_desde")
    )


FACT_DIMS = ("entidad_sk", *(d.sk for d in SCD1_DIMS))


def _silver_batch(settings: Settings, year: int) -> str:
    """Batch de silver del que sale el año: queda en los hechos como linaje."""
    return (
        storage.scan(settings.table(*SILVER))
        .filter(pl.col("ano_eje") == year)
        .select(pl.col("_batch_id").max())
        .collect()
        .item()
    )


def built_batches(settings: Settings) -> dict[int, str]:
    """Batch de silver con el que se construyó cada año de los hechos."""
    path = settings.table("gold", "fact_presupuesto_anual")
    if not storage.exists(path):
        return {}
    df = storage.scan(path).group_by("ano_eje").agg(pl.col("_batch_id").max()).collect()
    return dict(zip(df["ano_eje"].cast(pl.Int32).to_list(), df["_batch_id"].to_list(), strict=True))


def build_fact_ejecucion(settings: Settings, year: int) -> pl.DataFrame:
    lf = _attach_sks(_silver(settings).filter(pl.col("ano_eje") == year), settings)
    df = (
        lf.group_by("ano_eje", "periodo_sk", "sec_ejec", *(d.sk for d in SCD1_DIMS))
        .agg(*(pl.col(c).sum() for c in EXECUTION_COLUMNS))
        .collect(engine="streaming")
    )
    df = _entity_asof(df, settings, "periodo_sk").select(
        "ano_eje",
        "periodo_sk",
        *FACT_DIMS,
        *EXECUTION_COLUMNS,
        pl.lit(_silver_batch(settings, year)).alias("_batch_id"),
    )
    storage.overwrite(
        df,
        settings.table("gold", "fact_ejecucion_mensual"),
        partition_by=["ano_eje"],
        replace_where=f"ano_eje = {year}",
    )
    return df


def build_fact_presupuesto(settings: Settings, year: int) -> pl.DataFrame:
    mode = budget_semantics(settings, year)
    detail_keys = list(CODE_COLUMNS)
    base = _silver(settings).filter(pl.col("ano_eje") == year)
    budget = (
        base.sort("mes_eje")
        .group_by(["ano_eje", *detail_keys])
        .agg(
            *((pl.col(c).last() if mode == "latest" else pl.col(c).sum()) for c in BUDGET_COLUMNS),
            *(pl.col(c).sum() for c in EXECUTION_COLUMNS),
            pl.col("periodo_sk").max().alias("ultimo_periodo"),
        )
    )
    df = (
        _attach_sks(budget, settings)
        .group_by("ano_eje", "sec_ejec", *(d.sk for d in SCD1_DIMS))
        .agg(
            *(pl.col(c).sum() for c in (*BUDGET_COLUMNS, *EXECUTION_COLUMNS)),
            pl.col("ultimo_periodo").max(),
        )
        .collect(engine="streaming")
    )
    df = (
        _entity_asof(df, settings, "ultimo_periodo")
        .with_columns(
            modo_presupuesto=pl.lit(mode), _batch_id=pl.lit(_silver_batch(settings, year))
        )
        .select(
            "ano_eje",
            *FACT_DIMS,
            *BUDGET_COLUMNS,
            *EXECUTION_COLUMNS,
            "modo_presupuesto",
            "_batch_id",
        )
    )
    storage.overwrite(
        df,
        settings.table("gold", "fact_presupuesto_anual"),
        partition_by=["ano_eje"],
        replace_where=f"ano_eje = {year}",
    )
    return df


def reconcile(settings: Settings, year: int) -> dict:
    """Los totales de gold deben cuadrar al céntimo con silver. Si no, algo se perdió."""
    s = (
        _silver(settings)
        .filter(pl.col("ano_eje") == year)
        .select(pl.col("monto_devengado").sum())
        .collect()
        .item()
    )
    fe = (
        storage.scan(settings.table("gold", "fact_ejecucion_mensual"))
        .filter(pl.col("ano_eje") == year)
        .select(pl.col("monto_devengado").sum())
        .collect()
        .item()
    )
    fp = (
        storage.scan(settings.table("gold", "fact_presupuesto_anual"))
        .filter(pl.col("ano_eje") == year)
        .select(pl.col("monto_devengado").sum())
        .collect()
        .item()
    )
    orphans = (
        storage.scan(settings.table("gold", "fact_ejecucion_mensual"))
        .filter(pl.col("ano_eje") == year)
        .select(pl.any_horizontal(pl.col(c).is_null() for c in FACT_DIMS).sum())
        .collect()
        .item()
    )
    result = {"silver": s, "fact_ejecucion": fe, "fact_presupuesto": fp, "huerfanos": orphans}
    if not (s == fe == fp) or orphans:
        raise AssertionError(f"Reconciliación gold {year} fallida: {result}")
    log.info("reconciliación %s OK: devengado=%s", year, s)
    return result
