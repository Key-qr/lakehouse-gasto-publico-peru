"""Punto de entrada para Streamlit Community Cloud (usa este nombre de archivo por defecto).

El dashboard vive en dashboard/app.py; aquí solo se ejecuta.
"""

import runpy
from pathlib import Path

runpy.run_path(str(Path(__file__).parent / "dashboard" / "app.py"), run_name="__main__")
