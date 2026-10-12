# ADR 0001 · Arquitectura medallion sobre Delta Lake

- Estado: aceptada
- Fecha: 2026-10

## Contexto

El dataset "Presupuesto y Ejecución de Gasto" del MEF se publica como un CSV por año, de varios GB, que se republica completo cada mes mientras el año está abierto. Trae errores de origen (códigos fuera de catálogo, montos ilegibles, filas repetidas) y nombres de entidades que cambian en el tiempo.

Se necesita: conservar el dato original para auditoría, publicar una versión limpia y confiable, y servir un modelo analítico estable para consultas y dashboards.

## Decisión

Lakehouse en tres capas sobre tablas Delta:

| Capa | Contenido | Regla |
|---|---|---|
| bronze | CSV tal cual, todo texto, + metadatos de ingesta | append-only, nunca se corrige |
| silver | tipado, normalizado, deduplicado, al grano declarado | solo filas que pasan las reglas de severidad error |
| gold | modelo dimensional (Kimball) | reconciliado al céntimo con silver |

Más dos zonas de soporte: `quarantine/` (filas rechazadas con su motivo) y `_meta/` (manifiesto de ingesta, resultados de calidad, registro de corridas).

Delta aporta transacciones ACID, reemplazo atómico de particiones, time travel e historial de operaciones sobre archivos Parquet comunes.

## Consecuencias

- Cualquier número de gold se puede rastrear: hecho → `_batch_id` → manifiesto → archivo y SHA-256 de origen.
- Reprocesar es barato y seguro: bronze no se toca y silver/gold se recalculan por año.
- Hay redundancia de almacenamiento (bronze + silver). Aceptable: bronze en Parquet comprimido ocupa una fracción del CSV.
