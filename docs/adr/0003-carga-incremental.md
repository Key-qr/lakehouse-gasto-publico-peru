# ADR 0003 · Carga incremental: reemplazo de partición por año

- Estado: aceptada
- Fecha: 2026-10

## Contexto

El MEF no publica deltas: cada mes vuelve a publicar el archivo completo del año en curso. Entre una publicación y otra pueden aparecer filas nuevas, cambiar montos y también desaparecer filas (correcciones en el SIAF).

## Opciones

1. **MERGE por llave de negocio.** Inserta y actualiza bien, pero no borra lo que el origen eliminó salvo que se implemente `WHEN NOT MATCHED BY SOURCE` acotado al año, que es más costoso y frágil.
2. **Reemplazo de la partición del año** (`replaceWhere ano_eje = 2025`). El año queda exactamente igual a la última foto publicada.

## Decisión

Silver y los hechos de gold se cargan reemplazando la partición del año en una sola transacción Delta. MERGE se usa donde sí corresponde: el manifiesto de ingesta y las dimensiones SCD1, que se actualizan por llave y no deben perder filas de años anteriores.

Qué procesar se decide por estado, no por parámetros:
- silver procesa los años cuyo último batch del manifiesto no es el publicado en silver;
- gold procesa los años cuyo batch en silver no coincide con el `_batch_id` de los hechos, más los años con llaves de entidad desactualizadas (ver ADR 0005).

## Consecuencias

- Idempotente: correr el pipeline dos veces seguidas no cambia nada (la segunda corrida termina en `noop`).
- Si una corrida falla a la mitad, la siguiente retoma sola lo pendiente.
- Reprocesar un año reescribe toda su partición. Con el volumen anual del MEF es cuestión de minutos, así que no compensa la complejidad de un MERGE con borrados.
