"""CLI del lakehouse: ``lakehouse --help``."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

import polars as pl
import typer

from gasto_lakehouse import export, pipeline, storage
from gasto_lakehouse.config import Settings, load_sources
from gasto_lakehouse.ingestion import download as dl
from gasto_lakehouse.ingestion.sample import SampleSpec, write_sample

app = typer.Typer(help="Lakehouse del gasto público del Perú (MEF).", no_args_is_help=True)

TABLES = {
    "bronze": ["gasto"],
    "silver": ["gasto"],
    "quarantine": ["gasto"],
    "gold": list(export.GOLD_TABLES),
    "_meta": ["ingestion_manifest", "quality_results", "pipeline_runs"],
}


@app.callback()
def _setup(verbose: Annotated[bool, typer.Option("--verbose", "-v")] = False) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    pl.Config.set_tbl_rows(30)
    pl.Config.set_tbl_cols(12)
    pl.Config.set_fmt_str_lengths(60)


@app.command()
def sample(
    years: Annotated[list[int], typer.Option("--year", "-y")] = [2023, 2024],  # noqa: B006
    rows: Annotated[int, typer.Option(help="Filas por año")] = 50_000,
    months: Annotated[int, typer.Option(help="Meses publicados del último año")] = 12,
    seed: int = 42,
    clean: Annotated[bool, typer.Option(help="Sin defectos inyectados")] = False,
) -> None:
    """Genera CSV sintéticos con el contrato del MEF en landing."""
    s = Settings.from_env()
    s.ensure_dirs()
    for i, y in enumerate(sorted(years)):
        m = months if i == len(years) - 1 else 12
        p = write_sample(
            SampleSpec(year=y, rows=rows, months=m, seed=seed, dirty=not clean), s.landing
        )
        typer.echo(f"{p}  ({p.stat().st_size / 1e6:.1f} MB)")


@app.command()
def download(
    years: Annotated[list[int] | None, typer.Option("--year", "-y")] = None,
    all_years: Annotated[bool, typer.Option("--all")] = False,
) -> None:
    """Descarga los CSV oficiales del MEF a landing (reanudable)."""
    s = Settings.from_env()
    sources = load_sources(s.sources_file)
    if not all_years:
        if not years:
            raise typer.BadParameter("Indica --year AAAA (repetible) o --all")
        sources = [src for src in sources if src.year in years]
    for src in sources:
        dl.download(src.url, s.landing, src.filename)


@app.command()
def run(
    years: Annotated[list[int] | None, typer.Option("--year", "-y")] = None,
    force: Annotated[
        bool, typer.Option(help="Reprocesa aunque el archivo ya esté ingerido")
    ] = False,
    no_warehouse: bool = False,
) -> None:
    """Corre landing -> bronze -> silver -> gold -> warehouse."""
    report = pipeline.run(
        Settings.from_env(), years=years, force=force, build_warehouse=not no_warehouse
    )
    typer.echo(
        json.dumps(
            {
                "run_id": report.run_id,
                "status": report.status,
                "duration_s": report.duration_s,
                "silver": report.silver,
                "gold_years": report.gold_years,
            },
            indent=2,
            default=str,
            ensure_ascii=False,
        )
    )


@app.command()
def warehouse(
    demo_dir: Annotated[Path | None, typer.Option(help="Exporta marts a Parquet")] = None,
    fuente: Annotated[str, typer.Option(help="Texto de origen que muestra el dashboard")] = (
        "MEF - Plataforma Nacional de Datos Abiertos"
    ),
) -> None:
    """Reconstruye warehouse.duckdb desde gold (y opcionalmente exporta los marts)."""
    s = Settings.from_env()
    db = export.build_warehouse(s)
    if demo_dir:
        for p in export.export_marts(db, demo_dir):
            typer.echo(p)
        meta = {"fuente": fuente, "generado": datetime.now(UTC).isoformat(timespec="seconds")}
        (demo_dir / "_meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )


@app.command()
def status() -> None:
    """Versión y filas de cada tabla Delta."""
    s = Settings.from_env()
    rows = []
    for layer, names in TABLES.items():
        for n in names:
            path = s.table(layer, n)
            if storage.exists(path):
                count = storage.scan(path).select(pl.len()).collect().item()
                rows.append(
                    {"tabla": f"{layer}.{n}", "version": storage.version(path), "filas": count}
                )
    typer.echo(pl.DataFrame(rows) if rows else "Lakehouse vacío: corre `lakehouse run`")


@app.command()
def quality(last: int = 1) -> None:
    """Resultados de calidad de las últimas corridas."""
    df = storage.read(Settings.from_env().table("_meta", "quality_results"))
    runs = df.sort("evaluado_en", descending=True)["run_id"].unique(maintain_order=True).head(last)
    typer.echo(
        df.filter(pl.col("run_id").is_in(runs.to_list()))
        .select("ano_eje", "regla", "severidad", "filas_fallidas", "tasa_cumplimiento")
        .sort("ano_eje", "severidad", "regla")
    )


@app.command()
def history(
    table: Annotated[str, typer.Argument(help="p.ej. silver.gasto")], limit: int = 10
) -> None:
    """Historial de versiones Delta (time travel) de una tabla."""
    layer, name = table.split(".", 1)
    for h in storage.history(Settings.from_env().table(layer, name), limit):
        ts = datetime.fromtimestamp(h.get("timestamp", 0) / 1000, tz=UTC).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        typer.echo(
            f"v{h.get('version')}  {ts}  {h.get('operation')}  "
            f"{json.dumps(h.get('operationParameters', {}), ensure_ascii=False)[:120]}"
        )


@app.command()
def optimize(
    vacuum_hours: Annotated[int | None, typer.Option(help="Además ejecuta VACUUM")] = None,
) -> None:
    """Compacta archivos pequeños (OPTIMIZE) y Z-ORDER en los hechos."""
    s = Settings.from_env()
    zorder = {
        "fact_ejecucion_mensual": ["periodo_sk", "entidad_sk"],
        "fact_presupuesto_anual": ["entidad_sk"],
    }
    for layer, names in TABLES.items():
        for n in names:
            path = s.table(layer, n)
            if not storage.exists(path):
                continue
            m = storage.optimize(path, zorder.get(n))
            typer.echo(
                f"{layer}.{n}: +{m.get('numFilesAdded')} -{m.get('numFilesRemoved')} archivos"
            )
            if vacuum_hours is not None:
                removed = storage.vacuum(path, retention_hours=vacuum_hours, dry_run=False)
                typer.echo(f"  vacuum: {len(removed)} archivos eliminados")


if __name__ == "__main__":
    app()
