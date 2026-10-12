"""Motor de calidad de datos.

Cada regla es una expresión de Polars que vale True cuando la fila es válida. Las reglas se
evalúan todas juntas en una sola pasada (son columnas booleanas en el mismo plan lazy).

- severidad ``error``: la fila va a cuarentena con el motivo y no entra a silver.
- severidad ``warn``: la fila entra, pero el incumplimiento queda medido.

Si la proporción de filas en error supera ``max_error_ratio`` el lote completo se rechaza
(circuit breaker): mejor no publicar que publicar un año roto.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from gasto_lakehouse.schema import AMOUNT_COLUMNS, NIVELES_GOBIERNO


class QualityGateError(RuntimeError):
    """El lote supera el umbral de errores permitido."""


@dataclass(frozen=True)
class Rule:
    name: str
    severity: str  # error | warn
    description: str
    valid: pl.Expr


def _raw_amount_ok(col: str) -> pl.Expr:
    raw = pl.col(f"{col}__raw")
    return (raw == "") | pl.col(col).is_not_null()


def rules() -> list[Rule]:
    """Reglas sobre el DataFrame ya normalizado (ver silver.normalize)."""
    return [
        Rule(
            "llaves_no_nulas",
            "error",
            "Año, mes, entidad, nivel de gobierno y clasificador presentes",
            pl.all_horizontal(
                pl.col(c).is_not_null() & (pl.col(c) != "")
                for c in ("sec_ejec", "nivel_gobierno", "especifica_det", "generica")
            )
            & pl.col("ano_eje").is_not_null()
            & pl.col("mes_eje").is_not_null(),
        ),
        Rule(
            "ano_coincide_con_archivo",
            "error",
            "ANO_EJE igual al año del archivo de origen",
            pl.col("ano_eje").cast(pl.Int32) == pl.col("_source_year"),
        ),
        Rule(
            "mes_en_rango",
            "error",
            "MES_EJE entre 0 y 12",
            pl.col("mes_eje").is_between(0, 12),
        ),
        Rule(
            "nivel_gobierno_valido",
            "error",
            "NIVEL_GOBIERNO en {E, R, M}",
            pl.col("nivel_gobierno").is_in(list(NIVELES_GOBIERNO)),
        ),
        Rule(
            "montos_numericos",
            "error",
            "Todos los montos se pueden convertir a decimal",
            pl.all_horizontal(_raw_amount_ok(c) for c in AMOUNT_COLUMNS),
        ),
        Rule(
            "devengado_no_negativo",
            "warn",
            "Devengado >= 0 (los negativos suelen ser reversiones)",
            pl.col("monto_devengado").fill_null(0) >= 0,
        ),
        Rule(
            "girado_no_supera_comprometido",
            "warn",
            "Girado <= comprometido anual",
            pl.col("monto_girado").fill_null(0) <= pl.col("monto_comprometido_anual").fill_null(0),
        ),
    ]


def evaluate(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Agrega columnas ``_ok__<regla>`` y ``_motivo_error`` al plan."""
    rs = rules()
    lf = lf.with_columns(r.valid.fill_null(False).alias(f"_ok__{r.name}") for r in rs)
    errors = [r for r in rs if r.severity == "error"]
    return lf.with_columns(
        _motivo_error=pl.concat_str(
            [pl.when(~pl.col(f"_ok__{r.name}")).then(pl.lit(r.name)) for r in errors],
            separator=",",
            ignore_nulls=True,
        ),
        _es_valida=pl.all_horizontal(pl.col(f"_ok__{r.name}") for r in errors),
    )


def gate(failed_rows: int, total_rows: int, max_error_ratio: float) -> float:
    ratio = failed_rows / total_rows if total_rows else 0.0
    if ratio > max_error_ratio:
        raise QualityGateError(
            f"{failed_rows} de {total_rows} filas ({ratio:.2%}) fallan reglas de severidad "
            f"error; umbral permitido {max_error_ratio:.2%}. El lote no se publica en silver."
        )
    return ratio
