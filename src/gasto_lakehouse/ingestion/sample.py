"""Generador de datos sintéticos con el mismo contrato que los CSV del MEF.

Sirve para tres cosas: que cualquiera pruebe el proyecto sin bajar varios GB, que CI corra el
pipeline completo en segundos y que los tests tengan defectos conocidos (duplicados, montos
ilegibles, códigos inválidos, entidades renombradas) para verificar que cada capa los maneja.

Los catálogos (programas, funciones, fuentes, genéricas) usan nombres reales del clasificador
presupuestal peruano; los montos y las entidades son inventados.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

from gasto_lakehouse.schema import SOURCE_COLUMNS

DEPARTAMENTOS = [
    ("01", "AMAZONAS", "CHACHAPOYAS"),
    ("02", "ANCASH", "HUARAZ"),
    ("03", "APURIMAC", "ABANCAY"),
    ("04", "AREQUIPA", "AREQUIPA"),
    ("05", "AYACUCHO", "HUAMANGA"),
    ("06", "CAJAMARCA", "CAJAMARCA"),
    ("07", "PROVINCIA CONSTITUCIONAL DEL CALLAO", "CALLAO"),
    ("08", "CUSCO", "CUSCO"),
    ("09", "HUANCAVELICA", "HUANCAVELICA"),
    ("10", "HUANUCO", "HUANUCO"),
    ("11", "ICA", "ICA"),
    ("12", "JUNIN", "HUANCAYO"),
    ("13", "LA LIBERTAD", "TRUJILLO"),
    ("14", "LAMBAYEQUE", "CHICLAYO"),
    ("15", "LIMA", "LIMA"),
    ("16", "LORETO", "MAYNAS"),
    ("17", "MADRE DE DIOS", "TAMBOPATA"),
    ("18", "MOQUEGUA", "MARISCAL NIETO"),
    ("19", "PASCO", "PASCO"),
    ("20", "PIURA", "PIURA"),
    ("21", "PUNO", "PUNO"),
    ("22", "SAN MARTIN", "MOYOBAMBA"),
    ("23", "TACNA", "TACNA"),
    ("24", "TUMBES", "TUMBES"),
    ("25", "UCAYALI", "CORONEL PORTILLO"),
]

SECTORES = [
    ("10", "EDUCACION"),
    ("11", "SALUD"),
    ("36", "TRANSPORTES Y COMUNICACIONES"),
    ("07", "INTERIOR"),
    ("13", "AGRARIO Y DE RIEGO"),
    ("99", "GOBIERNOS REGIONALES"),
    ("98", "GOBIERNOS LOCALES"),
    ("09", "ECONOMIA Y FINANZAS"),
]

PROGRAMAS = [
    ("0001", "0001. PROGRAMA ARTICULADO NUTRICIONAL", "20", "SALUD"),
    ("0002", "0002. SALUD MATERNO NEONATAL", "20", "SALUD"),
    (
        "0090",
        "0090. LOGROS DE APRENDIZAJE DE ESTUDIANTES DE LA EDUCACION BASICA REGULAR",
        "22",
        "EDUCACION",
    ),
    (
        "0138",
        "0138. REDUCCION DEL COSTO, TIEMPO E INSEGURIDAD EN EL SISTEMA DE TRANSPORTE",
        "15",
        "TRANSPORTE",
    ),
    (
        "0042",
        "0042. APROVECHAMIENTO DE LOS RECURSOS HIDRICOS PARA USO AGRARIO",
        "10",
        "AGROPECUARIA",
    ),
    (
        "0030",
        "0030. REDUCCION DE DELITOS Y FALTAS QUE AFECTAN LA SEGURIDAD CIUDADANA",
        "05",
        "ORDEN PUBLICO Y SEGURIDAD",
    ),
    ("9001", "9001. ACCIONES CENTRALES", "03", "PLANEAMIENTO, GESTION Y RESERVA DE CONTINGENCIA"),
    (
        "9002",
        "9002. ASIGNACIONES PRESUPUESTARIAS QUE NO RESULTAN EN PRODUCTOS",
        "24",
        "PREVISION SOCIAL",
    ),
]

FUENTES = [
    ("1", "RECURSOS ORDINARIOS", "00", "RECURSOS ORDINARIOS", "0", "RECURSOS ORDINARIOS"),
    (
        "2",
        "RECURSOS DIRECTAMENTE RECAUDADOS",
        "09",
        "RECURSOS DIRECTAMENTE RECAUDADOS",
        "0",
        "RECURSOS DIRECTAMENTE RECAUDADOS",
    ),
    (
        "3",
        "RECURSOS POR OPERACIONES OFICIALES DE CREDITO",
        "19",
        "RECURSOS POR OPERACIONES OFICIALES DE CREDITO",
        "1",
        "ENDEUDAMIENTO INTERNO",
    ),
    (
        "4",
        "DONACIONES Y TRANSFERENCIAS",
        "13",
        "DONACIONES Y TRANSFERENCIAS",
        "0",
        "DONACIONES Y TRANSFERENCIAS",
    ),
    (
        "5",
        "RECURSOS DETERMINADOS",
        "18",
        "CANON Y SOBRECANON, REGALIAS, RENTA DE ADUANAS Y PARTICIPACIONES",
        "0",
        "CANON MINERO",
    ),
    ("5", "RECURSOS DETERMINADOS", "07", "FONDO DE COMPENSACION MUNICIPAL", "0", "FONCOMUN"),
]

GENERICAS = [
    ("1", "5-21: PERSONAL Y OBLIGACIONES SOCIALES", "5", "GASTOS CORRIENTES", 0.34),
    ("2", "5-22: PENSIONES Y OTRAS PRESTACIONES SOCIALES", "5", "GASTOS CORRIENTES", 0.08),
    ("3", "5-23: BIENES Y SERVICIOS", "5", "GASTOS CORRIENTES", 0.28),
    ("4", "5-24: DONACIONES Y TRANSFERENCIAS", "5", "GASTOS CORRIENTES", 0.06),
    ("5", "5-25: OTROS GASTOS", "5", "GASTOS CORRIENTES", 0.04),
    ("6", "6-26: ADQUISICION DE ACTIVOS NO FINANCIEROS", "6", "GASTOS DE CAPITAL", 0.20),
]

NIVELES = [("E", "GOBIERNO NACIONAL"), ("R", "GOBIERNOS REGIONALES"), ("M", "GOBIERNOS LOCALES")]

# Curva típica de ejecución en el Perú: arranque lento y pico en diciembre.
EXEC_CURVE = np.array([0.045, 0.06, 0.07, 0.075, 0.08, 0.08, 0.085, 0.085, 0.085, 0.09, 0.1, 0.145])


@dataclass(frozen=True)
class SampleSpec:
    year: int
    rows: int = 50_000
    months: int = 12
    entities: int = 120
    seed: int = 42
    dirty: bool = True


def _entities(rng: np.random.Generator, n: int) -> pl.DataFrame:
    rows = []
    for i in range(n):
        nivel, nivel_nombre = NIVELES[i % 3]
        dep_code, dep_name, capital = DEPARTAMENTOS[rng.integers(len(DEPARTAMENTOS))]
        if nivel == "E":
            sector, sector_nombre = SECTORES[rng.integers(0, 6)]
            if sector == "99":
                sector, sector_nombre = SECTORES[0]
            dep_code, dep_name, capital = DEPARTAMENTOS[14]
            nombre = f"{sector_nombre} - UNIDAD EJECUTORA {i:03d}"
        elif nivel == "R":
            sector, sector_nombre = SECTORES[5]
            nombre = f"REGION {dep_name} - SEDE {['CENTRAL', 'SALUD', 'EDUCACION', 'TRANSPORTES'][i % 4]}"
        else:
            sector, sector_nombre = SECTORES[6]
            nombre = f"MUNICIPALIDAD PROVINCIAL DE {capital} {i:03d}"
        sec_ejec = f"{300000 + i * 7}"
        rows.append(
            {
                "NIVEL_GOBIERNO": nivel,
                "NIVEL_GOBIERNO_NOMBRE": nivel_nombre,
                "SECTOR": sector,
                "SECTOR_NOMBRE": sector_nombre,
                "PLIEGO": f"{400 + (i % 40):03d}",
                "PLIEGO_NOMBRE": f"PLIEGO {sector_nombre} {400 + (i % 40):03d}",
                "SEC_EJEC": sec_ejec,
                "EJECUTORA": f"{i % 1000:03d}-{1000 + i}",
                "EJECUTORA_NOMBRE": nombre,
                "DEPARTAMENTO_EJECUTORA": dep_code,
                "DEPARTAMENTO_EJECUTORA_NOMBRE": dep_name,
                "PROVINCIA_EJECUTORA": "01",
                "PROVINCIA_EJECUTORA_NOMBRE": capital,
                "DISTRITO_EJECUTORA": "01",
                "DISTRITO_EJECUTORA_NOMBRE": capital,
            }
        )
    return pl.DataFrame(rows)


def generate(spec: SampleSpec) -> pl.DataFrame:
    """Devuelve un DataFrame con las 63 columnas del MEF, todo como texto (igual que el CSV)."""
    rng = np.random.default_rng(spec.seed + spec.year)
    months = np.arange(1, spec.months + 1)
    n_lines = max(spec.rows // spec.months, 1)

    # Las entidades son las mismas todos los años (semilla fija): solo cambian por los
    # eventos controlados de _inject_defects.
    ent = _entities(np.random.default_rng(spec.seed), spec.entities)
    e_idx = rng.integers(0, ent.height, n_lines)
    p_idx = rng.integers(0, len(PROGRAMAS), n_lines)
    f_idx = rng.choice(len(FUENTES), n_lines, p=[0.55, 0.12, 0.05, 0.08, 0.12, 0.08])
    g_idx = rng.choice(len(GENERICAS), n_lines, p=[g[4] for g in GENERICAS])
    sub = rng.integers(1, 4, n_lines)
    esp = rng.integers(1, 6, n_lines)
    is_proj = (g_idx == 5) & (rng.random(n_lines) < 0.8)

    pia = np.round(rng.lognormal(mean=11.0, sigma=1.4, size=n_lines), 2)
    pia = np.where(is_proj, pia * 6, pia)
    pim_final = np.round(pia * rng.uniform(0.85, 1.45, n_lines), 2)
    exec_rate = np.clip(rng.beta(6, 2, n_lines) * np.where(is_proj, 0.85, 1.0), 0.05, 1.0)

    lines = ent[e_idx.tolist()].with_columns(
        _line=pl.Series(np.arange(n_lines)),
        _p=pl.Series(p_idx),
        _f=pl.Series(f_idx),
        _g=pl.Series(g_idx),
        _sub=pl.Series(sub),
        _esp=pl.Series(esp),
        _proj=pl.Series(is_proj),
        _pia=pl.Series(pia),
        _pimf=pl.Series(pim_final),
        _rate=pl.Series(exec_rate),
    )

    prog = pl.DataFrame(
        PROGRAMAS,
        schema=["PROGRAMA_PPTO", "PROGRAMA_PPTO_NOMBRE", "FUNCION", "FUNCION_NOMBRE"],
        orient="row",
    ).with_row_index("_p")
    fuen = pl.DataFrame(
        FUENTES,
        schema=[
            "FUENTE_FINANCIAMIENTO",
            "FUENTE_FINANCIAMIENTO_NOMBRE",
            "RUBRO",
            "RUBRO_NOMBRE",
            "TIPO_RECURSO",
            "TIPO_RECURSO_NOMBRE",
        ],
        orient="row",
    ).with_row_index("_f")
    gen = pl.DataFrame(
        [g[:4] for g in GENERICAS],
        schema=["GENERICA", "GENERICA_NOMBRE", "CATEGORIA_GASTO", "CATEGORIA_GASTO_NOMBRE"],
        orient="row",
    ).with_row_index("_g")

    lines = (
        lines.join(prog.with_columns(pl.col("_p").cast(pl.Int64)), on="_p")
        .join(fuen.with_columns(pl.col("_f").cast(pl.Int64)), on="_f")
        .join(gen.with_columns(pl.col("_g").cast(pl.Int64)), on="_g")
    )

    df = lines.join(pl.DataFrame({"MES_EJE": months}), how="cross")
    m = df["MES_EJE"].to_numpy() - 1
    n = df.height
    curve = EXEC_CURVE[: spec.months] / EXEC_CURVE.sum()
    rate = df["_rate"].to_numpy()
    pimf = df["_pimf"].to_numpy()
    pia_r = df["_pia"].to_numpy()
    noise = rng.uniform(0.6, 1.4, n)

    # PIM como stock: se repite cada mes y crece con las modificaciones presupuestales.
    pim_month = np.round(pia_r + (pimf - pia_r) * np.minimum(1.0, (m + 1) / 9), 2)
    dev = np.round(pimf * rate * curve[m] * noise, 2)
    gir = np.round(dev * rng.uniform(0.85, 1.0, n), 2)
    comp = np.round(dev * rng.uniform(1.0, 1.08, n), 2)
    cert = np.round(comp * rng.uniform(1.0, 1.1, n), 2)
    comp_anual = np.round(comp * rng.uniform(1.0, 1.15, n), 2)

    is_proj_rows = df["_proj"].to_numpy()
    line = df["_line"].to_numpy()
    out = df.with_columns(
        ANO_EJE=pl.lit(str(spec.year)),
        MES_EJE=pl.col("MES_EJE").cast(pl.String),
        SEC_FUNC=pl.Series((line % 400 + 1).astype(str)),
        TIPO_ACT_PROY=pl.Series(np.where(is_proj_rows, "2", "3")),
        TIPO_ACT_PROY_NOMBRE=pl.Series(np.where(is_proj_rows, "PROYECTO", "PRODUCTO")),
        PRODUCTO_PROYECTO=pl.Series(
            np.where(
                is_proj_rows,
                (2_400_000 + line % 900).astype(str),
                (3_000_000 + line % 50).astype(str),
            )
        ),
        PRODUCTO_PROYECTO_NOMBRE=pl.Series(
            np.where(
                is_proj_rows,
                np.char.add(
                    "MEJORAMIENTO DE INFRAESTRUCTURA VIAL TRAMO ", (line % 900).astype(str)
                ),
                np.char.add("PRODUCTO OPERATIVO ", (line % 50).astype(str)),
            )
        ),
        ACTIVIDAD_ACCION_OBRA=pl.Series((5_000_000 + line % 300).astype(str)),
        ACTIVIDAD_ACCION_OBRA_NOMBRE=pl.Series(np.char.add("ACCION ", (line % 300).astype(str))),
        DIVISION_FUNCIONAL=pl.col("FUNCION") + pl.lit("0"),
        DIVISION_FUNCIONAL_NOMBRE=pl.lit("DIVISION ") + pl.col("FUNCION_NOMBRE"),
        GRUPO_FUNCIONAL=pl.col("FUNCION") + pl.lit("00"),
        GRUPO_FUNCIONAL_NOMBRE=pl.lit("GRUPO ") + pl.col("FUNCION_NOMBRE"),
        META=pl.Series(np.char.zfill((line % 30 + 1).astype(str), 5)),
        FINALIDAD=pl.Series(np.char.zfill((line % 5000).astype(str), 7)),
        META_NOMBRE=pl.Series(np.char.add("META OPERATIVA Nº ", (line % 30 + 1).astype(str))),
        DEPARTAMENTO_META=pl.col("DEPARTAMENTO_EJECUTORA"),
        DEPARTAMENTO_META_NOMBRE=pl.col("DEPARTAMENTO_EJECUTORA_NOMBRE"),
        TIPO_TRANSACCION=pl.lit("2"),
        SUBGENERICA=pl.col("_sub").cast(pl.String),
        SUBGENERICA_NOMBRE=pl.lit("SUBGENERICA ") + pl.col("_sub").cast(pl.String),
        SUBGENERICA_DET=pl.col("_sub").cast(pl.String),
        SUBGENERICA_DET_NOMBRE=pl.lit("SUBGENERICA DETALLE ") + pl.col("_sub").cast(pl.String),
        ESPECIFICA=pl.col("_esp").cast(pl.String),
        ESPECIFICA_NOMBRE=pl.lit("ESPECIFICA ") + pl.col("_esp").cast(pl.String),
        ESPECIFICA_DET=pl.col("_esp").cast(pl.String),
        ESPECIFICA_DET_NOMBRE=pl.lit("ESPECIFICA DETALLE ") + pl.col("_esp").cast(pl.String),
        MONTO_PIA=pl.Series(pia_r).round(2).cast(pl.String),
        MONTO_PIM=pl.Series(pim_month).cast(pl.String),
        MONTO_CERTIFICADO=pl.Series(cert).cast(pl.String),
        MONTO_COMPROMETIDO_ANUAL=pl.Series(comp_anual).cast(pl.String),
        MONTO_COMPROMETIDO=pl.Series(comp).cast(pl.String),
        MONTO_DEVENGADO=pl.Series(dev).cast(pl.String),
        MONTO_GIRADO=pl.Series(gir).cast(pl.String),
    ).select(SOURCE_COLUMNS)

    if spec.dirty:
        out = _inject_defects(out, rng, spec.year)
    return out


# Eventos de negocio que generan historia SCD2 (periodo AAAAMM desde el que rigen).
RENAME_FROM = 202307
RELOCATE_FROM = 202304


def _inject_defects(df: pl.DataFrame, rng: np.random.Generator, year: int) -> pl.DataFrame:
    """Defectos realistas y controlados. Cada uno tiene un test que verifica su tratamiento."""
    n = df.height
    df = df.with_row_index("_i")

    def pick(frac: float, minimum: int = 1) -> set[int]:
        k = max(int(n * frac), minimum)
        return set(rng.choice(n, size=min(k, n), replace=False).tolist())

    messy_names = pick(0.01)
    bad_nivel = pick(0.0005)
    bad_month = pick(0.0005)
    bad_amount = pick(0.0005)
    negative = pick(0.001)

    i = pl.col("_i")
    df = df.with_columns(
        EJECUTORA_NOMBRE=pl.when(i.is_in(list(messy_names)))
        .then(pl.lit("  ") + pl.col("EJECUTORA_NOMBRE").str.to_lowercase() + pl.lit("   "))
        .otherwise(pl.col("EJECUTORA_NOMBRE")),
        NIVEL_GOBIERNO=pl.when(i.is_in(list(bad_nivel)))
        .then(pl.lit("X"))
        .otherwise(pl.col("NIVEL_GOBIERNO")),
        MES_EJE=pl.when(i.is_in(list(bad_month))).then(pl.lit("13")).otherwise(pl.col("MES_EJE")),
        MONTO_DEVENGADO=pl.when(i.is_in(list(bad_amount)))
        .then(pl.lit("N/D"))
        .when(i.is_in(list(negative)))
        .then(pl.lit("-") + pl.col("MONTO_DEVENGADO"))
        .otherwise(pl.col("MONTO_DEVENGADO")),
    )

    # Entidades que cambian de nombre y de provincia -> versiones SCD2 en gold.
    sec = df["SEC_EJEC"].unique().sort().head(3).to_list()
    periodo = year * 100 + pl.col("MES_EJE").cast(pl.Int32, strict=False)
    df = df.with_columns(
        EJECUTORA_NOMBRE=pl.when(pl.col("SEC_EJEC").is_in(sec[:2]) & (periodo >= RENAME_FROM))
        .then(
            pl.col("EJECUTORA_NOMBRE").str.strip_chars().str.to_uppercase()
            + pl.lit(" (REORGANIZADA)")
        )
        .otherwise(pl.col("EJECUTORA_NOMBRE")),
        PROVINCIA_EJECUTORA=pl.when((pl.col("SEC_EJEC") == sec[2]) & (periodo >= RELOCATE_FROM))
        .then(pl.lit("02"))
        .otherwise(pl.col("PROVINCIA_EJECUTORA")),
    )

    # Filas duplicadas exactas (reenvíos del sistema fuente).
    dup = df.filter(i.is_in(list(pick(0.003))))
    return pl.concat([df, dup]).drop("_i")


def write_sample(spec: SampleSpec, landing: Path, encoding: str = "utf-8") -> Path:
    landing.mkdir(parents=True, exist_ok=True)
    path = landing / f"{spec.year}-Gasto.csv"
    df = generate(spec)
    if encoding.lower().replace("_", "-") in ("utf-8", "utf8"):
        df.write_csv(path)
    else:
        tmp = path.with_suffix(".utf8.tmp")
        df.write_csv(tmp)
        with open(tmp, encoding="utf-8") as src, open(path, "w", encoding=encoding) as dst:
            for chunk in iter(lambda: src.read(1 << 20), ""):
                dst.write(chunk)
        tmp.unlink()
    return path
