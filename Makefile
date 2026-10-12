# Atajos de desarrollo. En Windows sin make, cada objetivo es un comando que puedes copiar.
PY ?= python
YEAR ?= 2024

.PHONY: install sample run demo demo-data download dashboard dagster test lint fmt benchmark docker-up clean

install:            ## Entorno de desarrollo completo
	$(PY) -m pip install -e ".[dev,dashboard,orchestration]"

sample:             ## CSV sintéticos con el contrato del MEF
	lakehouse sample -y 2023 -y 2024 -y 2025 --rows 80000 --months 9

run:                ## landing -> bronze -> silver -> gold -> warehouse
	lakehouse run

demo: sample run demo-data   ## Todo de punta a punta con datos de muestra

demo-data:          ## Exporta los marts para el dashboard publicado
	lakehouse warehouse --demo-dir dashboard/data

download:           ## Baja un año real del MEF: make download YEAR=2024
	lakehouse download -y $(YEAR)

dashboard:
	streamlit run dashboard/app.py

dagster:
	dagster dev -m gasto_lakehouse.orchestration

test:
	pytest --cov=gasto_lakehouse --cov-report=term-missing

lint:
	ruff check src tests dashboard scripts
	ruff format --check src tests dashboard scripts

fmt:
	ruff check --fix src tests dashboard scripts
	ruff format src tests dashboard scripts

benchmark:          ## Mide el pipeline con volumen alto: make benchmark ROWS=2000000
	$(PY) scripts/benchmark.py --rows $${ROWS:-1000000}

docker-up:
	docker compose up --build

clean:
	rm -rf data/lakehouse data/warehouse.duckdb .pytest_cache .ruff_cache .coverage
