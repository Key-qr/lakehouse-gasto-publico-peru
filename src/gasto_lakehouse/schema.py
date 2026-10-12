"""Contrato de datos del dataset "Presupuesto y Ejecución de Gasto" del MEF.

Las 63 columnas salen del diccionario oficial (Gastos_Diccionario.csv). Este módulo es la
única fuente de verdad sobre nombres, tipos y grano; bronze, silver, gold, el generador de
muestra y los tests lo importan de aquí.
"""

from __future__ import annotations

import polars as pl

# Orden exacto del diccionario oficial.
SOURCE_COLUMNS: tuple[str, ...] = (
    "ANO_EJE",
    "MES_EJE",
    "NIVEL_GOBIERNO",
    "NIVEL_GOBIERNO_NOMBRE",
    "SECTOR",
    "SECTOR_NOMBRE",
    "PLIEGO",
    "PLIEGO_NOMBRE",
    "SEC_EJEC",
    "EJECUTORA",
    "EJECUTORA_NOMBRE",
    "DEPARTAMENTO_EJECUTORA",
    "DEPARTAMENTO_EJECUTORA_NOMBRE",
    "PROVINCIA_EJECUTORA",
    "PROVINCIA_EJECUTORA_NOMBRE",
    "DISTRITO_EJECUTORA",
    "DISTRITO_EJECUTORA_NOMBRE",
    "SEC_FUNC",
    "PROGRAMA_PPTO",
    "PROGRAMA_PPTO_NOMBRE",
    "TIPO_ACT_PROY",
    "TIPO_ACT_PROY_NOMBRE",
    "PRODUCTO_PROYECTO",
    "PRODUCTO_PROYECTO_NOMBRE",
    "ACTIVIDAD_ACCION_OBRA",
    "ACTIVIDAD_ACCION_OBRA_NOMBRE",
    "FUNCION",
    "FUNCION_NOMBRE",
    "DIVISION_FUNCIONAL",
    "DIVISION_FUNCIONAL_NOMBRE",
    "GRUPO_FUNCIONAL",
    "GRUPO_FUNCIONAL_NOMBRE",
    "META",
    "FINALIDAD",
    "META_NOMBRE",
    "DEPARTAMENTO_META",
    "DEPARTAMENTO_META_NOMBRE",
    "FUENTE_FINANCIAMIENTO",
    "FUENTE_FINANCIAMIENTO_NOMBRE",
    "RUBRO",
    "RUBRO_NOMBRE",
    "TIPO_RECURSO",
    "TIPO_RECURSO_NOMBRE",
    "CATEGORIA_GASTO",
    "CATEGORIA_GASTO_NOMBRE",
    "TIPO_TRANSACCION",
    "GENERICA",
    "GENERICA_NOMBRE",
    "SUBGENERICA",
    "SUBGENERICA_NOMBRE",
    "SUBGENERICA_DET",
    "SUBGENERICA_DET_NOMBRE",
    "ESPECIFICA",
    "ESPECIFICA_NOMBRE",
    "ESPECIFICA_DET",
    "ESPECIFICA_DET_NOMBRE",
    "MONTO_PIA",
    "MONTO_PIM",
    "MONTO_CERTIFICADO",
    "MONTO_COMPROMETIDO_ANUAL",
    "MONTO_COMPROMETIDO",
    "MONTO_DEVENGADO",
    "MONTO_GIRADO",
)

assert len(SOURCE_COLUMNS) == 63


def snake(name: str) -> str:
    return name.strip().lower()


COLUMNS: tuple[str, ...] = tuple(snake(c) for c in SOURCE_COLUMNS)

# Montos: PIA y PIM son presupuesto (stock); el resto son fases de ejecución (flujo mensual).
BUDGET_COLUMNS: tuple[str, ...] = ("monto_pia", "monto_pim")
EXECUTION_COLUMNS: tuple[str, ...] = (
    "monto_certificado",
    "monto_comprometido_anual",
    "monto_comprometido",
    "monto_devengado",
    "monto_girado",
)
AMOUNT_COLUMNS: tuple[str, ...] = BUDGET_COLUMNS + EXECUTION_COLUMNS

NAME_COLUMNS: tuple[str, ...] = tuple(c for c in COLUMNS if c.endswith("_nombre"))
PERIOD_COLUMNS: tuple[str, ...] = ("ano_eje", "mes_eje")
CODE_COLUMNS: tuple[str, ...] = tuple(
    c for c in COLUMNS if c not in NAME_COLUMNS + AMOUNT_COLUMNS + PERIOD_COLUMNS
)

# Grano de silver: un registro por periodo y combinación completa de códigos.
GRAIN: tuple[str, ...] = PERIOD_COLUMNS + CODE_COLUMNS

# Precisión monetaria. Soles con 2 decimales; 20 dígitos cubren con holgura el PIM nacional.
MONEY = pl.Decimal(precision=20, scale=2)

SILVER_SCHEMA: dict[str, pl.DataType] = {
    "ano_eje": pl.Int16(),
    "mes_eje": pl.Int8(),
    **{c: pl.String() for c in CODE_COLUMNS},
    **{c: pl.String() for c in NAME_COLUMNS},
    **{c: MONEY for c in AMOUNT_COLUMNS},
}

# Columnas técnicas que bronze agrega a cada fila.
BRONZE_META_COLUMNS: tuple[str, ...] = (
    "_batch_id",
    "_source_file",
    "_source_year",
    "_ingested_at",
    "_row_number",
)

NIVELES_GOBIERNO = {"E": "GOBIERNO NACIONAL", "R": "GOBIERNOS REGIONALES", "M": "GOBIERNOS LOCALES"}
