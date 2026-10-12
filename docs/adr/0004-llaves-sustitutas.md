# ADR 0004 · Llaves sustitutas por hash determinista

- Estado: aceptada
- Fecha: 2026-10

## Contexto

Las dimensiones necesitan llaves sustitutas. Lo clásico es una secuencia (IDENTITY), pero obliga a leer la dimensión antes de asignar llaves, complica reconstrucciones y hace que el mismo dato reciba llaves distintas en entornos distintos.

## Decisión

`sk = int(md5(llave_natural_concatenada)[:15], 16)`: un entero de 60 bits, positivo y estable.

- SCD1: la llave natural es el código (p.ej. `fuente_financiamiento|rubro|tipo_recurso`).
- SCD2 (`dim_entidad`): `sec_ejec|valido_desde`, así cada versión tiene su propia llave.

No se usa `pl.Expr.hash` porque Polars no garantiza que ese hash sea estable entre versiones.

## Consecuencias

- La misma entrada produce las mismas llaves en cualquier máquina y corrida; los hechos se pueden recalcular por año sin tocar dimensiones.
- Riesgo de colisión con 60 bits y decenas de miles de miembros por dimensión: despreciable.
- El hash se calcula solo sobre los miembros únicos de cada dimensión (miles de filas); los hechos obtienen la llave por join.
