# ADR 0007 · Motor de calidad propio con cuarentena y circuit breaker

- Estado: aceptada
- Fecha: 2026-10

## Contexto

Hace falta validar millones de filas por año, separar las malas sin perderlas y medir cada regla en el tiempo.

## Opciones

- **Great Expectations / Soda**: completos, pero agregan mucha dependencia y configuración, y evalúan sobre pandas o Spark; con Polars hay que convertir o usar integraciones parciales.
- **Reglas como expresiones Polars**: cada regla es una columna booleana; todas se evalúan en una sola pasada del mismo plan lazy, en streaming.

## Decisión

Reglas propias (`quality.py`) con dos severidades:

| Severidad | Efecto |
|---|---|
| error | la fila va a `quarantine/gasto` con el motivo (`_motivo_error`) y su número de fila en el CSV |
| warn | la fila entra a silver; el incumplimiento se mide |

Cada evaluación se guarda en `_meta/quality_results` (regla, filas evaluadas, filas fallidas, tasa de cumplimiento, corrida). Si las filas en error superan `LAKEHOUSE_MAX_ERROR_RATIO` (5 % por defecto), el año se rechaza completo y silver no se modifica.

Además, gold verifica que el devengado cuadre al céntimo entre silver y los dos hechos, y que ningún hecho quede sin dimensión.

## Consecuencias

- Agregar una regla es agregar una expresión y su test.
- Las reglas se exponen como asset checks en Dagster.
- Lo que GE trae de serie (data docs, perfiles) aquí no existe; el historial de `_meta/quality_results` cubre la parte que importa para operar.
