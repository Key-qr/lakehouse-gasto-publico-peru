# ADR 0002 · Polars + delta-rs + DuckDB en un solo nodo, en lugar de Spark

- Estado: aceptada
- Fecha: 2026-10

## Contexto

Volumen: cada año del MEF son algunos millones de filas y unos pocos GB en CSV. Toda la historia 2012-2026 está en el orden de decenas de GB de CSV, bastante menos ya en Parquet comprimido.

El proyecto tiene que correr en la laptop de cualquiera que clone el repo, incluido Windows sin WSL, y en un runner gratuito de GitHub Actions.

## Opciones

| Opción | A favor | En contra |
|---|---|---|
| Spark + Delta (delta-spark) | estándar de la industria, escala horizontal | JVM, configuración de Hadoop en Windows, arranque lento, sobredimensionado para este volumen |
| **Polars + delta-rs + DuckDB** | sin JVM, `pip install` y listo, motor vectorizado y streaming, muy rápido en un nodo | no escala horizontalmente |
| Pandas | conocido | sin ejecución lazy ni streaming, se queda sin memoria con un año completo |

## Decisión

Polars como motor de transformación (planes lazy y motor streaming), delta-rs para escribir y leer Delta, DuckDB para la capa semántica en SQL y el consumo.

Para acotar memoria con archivos grandes:
- bronze lee el CSV en bloques de 16 MB con `pyarrow.csv.open_csv` y escribe en streaming;
- silver normaliza y evalúa calidad una sola vez con el motor streaming de Polars hacia un Parquet temporal, y desde ahí procesa el año mes a mes (el mes es parte del grano), escribiendo todos los meses en una sola transacción.

Medido en `docs/benchmark.md`: un año de 2 millones de filas (CSV de 1,4 GB) pasa de punta a punta en menos de un minuto con 2 vCPU y ~3,3 GB de memoria pico.

## Consecuencias

- El acceso a Delta está aislado en `storage.py`. Las tablas son Delta estándar: Spark, Databricks, Trino o DuckDB pueden leerlas sin conversión. Si el volumen creciera un orden de magnitud, las transformaciones se portan a PySpark y el layout se mantiene.
- El pipeline corre igual en local, Docker y CI sin servicios externos.
