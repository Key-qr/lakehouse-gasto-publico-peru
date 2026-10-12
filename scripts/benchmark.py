"""Benchmark del pipeline con volumen alto de datos sintéticos.

    python scripts/benchmark.py --rows 2000000

Mide tiempo por etapa y memoria pico del proceso. Los resultados de referencia están en
docs/benchmark.md.
"""

from __future__ import annotations

import argparse
import platform
import resource
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path

from gasto_lakehouse import bronze, export, pipeline, silver
from gasto_lakehouse.config import Settings


def peak_mb() -> float:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss / 1024 / 1024 if sys.platform == "darwin" else rss / 1024


def _generate_in_subprocess(args: argparse.Namespace, landing: Path) -> Path:
    code = (
        "import sys; from pathlib import Path;"
        "from gasto_lakehouse.ingestion.sample import SampleSpec, write_sample;"
        f"write_sample(SampleSpec(year={args.year}, rows={args.rows}), Path(r'{landing}'))"
    )
    subprocess.run([sys.executable, "-c", code], check=True)
    return landing / f"{args.year}-Gasto.csv"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--year", type=int, default=2024)
    ap.add_argument("--root", type=Path, default=None)
    ap.add_argument("--csv", type=Path, default=None, help="Reusar un CSV ya generado")
    args = ap.parse_args()

    root = args.root or Path(tempfile.mkdtemp(prefix="lakehouse_bench_"))
    s = Settings(root=root, sources_file=Settings.from_env().sources_file)
    s.ensure_dirs()
    timings: list[tuple[str, float, float]] = []

    def step(name, fn):
        t = time.perf_counter()
        out = fn()
        timings.append((name, time.perf_counter() - t, peak_mb()))
        return out

    if args.csv:
        csv = s.landing / args.csv.name
        if not csv.exists():
            shutil.copy(args.csv, csv)
    else:
        # En un subproceso: así la memoria pico medida es solo la del pipeline.
        csv = step("generar CSV", lambda: _generate_in_subprocess(args, s.landing))
    b = step("bronze", lambda: bronze.ingest_file(s, csv))
    r = step("silver", lambda: silver.build_year(s, args.year, b.batch_id, uuid.uuid4().hex))
    step("gold dimensiones", lambda: pipeline.stage_gold_dims(s))
    step("gold hechos", lambda: pipeline.stage_gold_facts(s, {args.year}))
    step("warehouse", lambda: export.build_warehouse(s))

    size_mb = csv.stat().st_size / 1e6
    total = sum(t for name, t, _ in timings if name != "generar CSV")
    print(f"\nFilas: {b.rows:,} | CSV: {size_mb:,.0f} MB | silver: {r.rows_out:,} filas")
    machine = platform.processor() or platform.machine()
    print(f"Máquina: {machine} | Python {platform.python_version()}\n")
    print("| etapa | segundos | memoria pico acumulada (MB) |\n|---|---:|---:|")
    for name, t, mem in timings:
        print(f"| {name} | {t:,.1f} | {mem:,.0f} |")
    print(f"| **pipeline (sin generar CSV)** | **{total:,.1f}** | |")
    print(f"\nThroughput: {b.rows / total:,.0f} filas/s | memoria pico: {peak_mb():,.0f} MB")
    print(f"Lakehouse en: {root}")


if __name__ == "__main__":
    main()
