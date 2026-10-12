"""Configuración del lakehouse.

Todo se resuelve desde variables de entorno para que el mismo código corra igual en local,
en Docker y en CI. Las rutas apuntan a disco local; cambiar ``LAKEHOUSE_ROOT`` es lo único
necesario para mover el lakehouse a otro volumen.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

import yaml


def _default_sources() -> Path:
    candidates = [
        Path.cwd() / "config" / "sources.yaml",
        Path(__file__).resolve().parents[2] / "config" / "sources.yaml",
    ]
    return next((c for c in candidates if c.exists()), candidates[0])


@dataclass(frozen=True)
class Settings:
    root: Path
    sources_file: Path
    max_error_ratio: float = 0.05
    budget_semantics: str = "auto"  # auto | latest | sum
    csv_block_size: int = 16 * 1024 * 1024
    extra: dict = field(default_factory=dict)

    # --- zonas del lakehouse -------------------------------------------------
    @property
    def landing(self) -> Path:
        return self.root / "landing"

    @property
    def lakehouse(self) -> Path:
        return self.root / "lakehouse"

    @property
    def warehouse_db(self) -> Path:
        return self.root / "warehouse.duckdb"

    def table(self, layer: str, name: str) -> str:
        """Ruta de una tabla Delta, p.ej. ``table("silver", "gasto")``."""
        return str(self.lakehouse / layer / name)

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            root=Path(os.getenv("LAKEHOUSE_ROOT", "data")).resolve(),
            sources_file=Path(os.getenv("LAKEHOUSE_SOURCES") or _default_sources()),
            max_error_ratio=float(os.getenv("LAKEHOUSE_MAX_ERROR_RATIO", "0.05")),
            budget_semantics=os.getenv("LAKEHOUSE_BUDGET_SEMANTICS", "auto"),
        )

    def ensure_dirs(self) -> None:
        for p in (self.landing, self.lakehouse):
            p.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class Source:
    year: int
    url: str
    kind: str  # anual | mensual

    @property
    def filename(self) -> str:
        return self.url.rsplit("/", 1)[-1]


def load_sources(path: Path) -> list[Source]:
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return [
        Source(year=int(s["year"]), url=s["url"], kind=s.get("kind", "anual"))
        for s in raw["sources"]
    ]
