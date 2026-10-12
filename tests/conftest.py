from __future__ import annotations

from pathlib import Path

import pytest

from gasto_lakehouse import pipeline
from gasto_lakehouse.config import Settings
from gasto_lakehouse.ingestion.sample import SampleSpec, write_sample

SOURCES = Path(__file__).resolve().parents[1] / "config" / "sources.yaml"


def make_settings(root: Path, **kw) -> Settings:
    s = Settings(root=root, sources_file=SOURCES, **kw)
    s.ensure_dirs()
    return s


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return make_settings(tmp_path / "lh")


@pytest.fixture(scope="session")
def built(tmp_path_factory: pytest.TempPathFactory) -> Settings:
    """Lakehouse completo con dos años de muestra, construido una vez por sesión."""
    s = make_settings(tmp_path_factory.mktemp("lh_built"))
    write_sample(SampleSpec(year=2023, rows=12_000), s.landing)
    write_sample(SampleSpec(year=2024, rows=9_000, months=9), s.landing)
    report = pipeline.run(s)
    assert report.status == "success"
    return s
