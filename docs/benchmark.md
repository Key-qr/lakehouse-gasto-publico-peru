# Benchmark

Un año sintético con el contrato del MEF y defectos inyectados, procesado de punta a punta con `scripts/benchmark.py`.

**Volumen:** 2.005.991 filas · CSV de 1.401 MB · 63 columnas
**Máquina:** VM de 2 vCPU (Intel Xeon 2,1 GHz), 8 GB de RAM · Python 3.13 · Polars 2.0 · deltalake 1.6

| etapa | segundos | memoria pico acumulada (MB) |
|---|---:|---:|
| bronze (CSV → Delta) | 9,0 | 1.412 |
| silver (calidad, cuarentena, dedup, grano) | 32,9 | 3.063 |
| gold dimensiones (SCD1 + SCD2) | 1,9 | 3.063 |
| gold hechos + reconciliación | 6,7 | 3.318 |
| warehouse DuckDB + marts SQL | 2,8 | 3.318 |
| **total** | **53,3** | |

**Throughput:** ~37.600 filas/s.

## Almacenamiento

| zona | tamaño |
|---|---:|
| CSV de origen | 1.401 MB |
| bronze (Parquet zstd vía Delta) | 104 MB |
| silver | 127 MB |
| gold | 75 MB |
| warehouse.duckdb | 146 MB |

El CSV se reduce más de 13 veces al pasar a bronze: el grueso del texto son nombres repetidos que Parquet comprime por diccionario.

## Qué se optimizó

La primera versión de silver tardaba 165 s en la etapa: leía bronze y normalizaba otra vez para cada uno de los 12 meses. Perfilando, normalizar (regex sobre ~55 columnas de texto) era el 90 % del costo. Ahora se normaliza una vez, en streaming, a un Parquet de trabajo y el bucle mensual lee de ahí: **165 s → 33 s**.

En bronze, bajar el bloque de lectura de PyArrow de 64 MB a 16 MB redujo la memoria pico de la etapa de ~2,4 GB a ~1,4 GB sin costo en tiempo (el lector precarga varios bloques en paralelo).

## Reproducir

```bash
python scripts/benchmark.py --rows 2000000
# o reusando un CSV ya generado / descargado
python scripts/benchmark.py --csv data/landing/2024-Gasto.csv
```
