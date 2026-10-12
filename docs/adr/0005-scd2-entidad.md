# ADR 0005 · dim_entidad como SCD tipo 2 reconstruida desde silver

- Estado: aceptada
- Fecha: 2026-10

## Contexto

Las unidades ejecutoras (`SEC_EJEC`) cambian de nombre, de pliego o de ubicación con el tiempo. Para analizar el gasto "como era" en cada periodo, la dimensión debe guardar historia.

## Opciones

1. **SCD2 incremental con MERGE**: cerrar la versión vigente y abrir una nueva cuando llega un cambio. Funciona si los datos llegan en orden cronológico; se rompe al recargar un año pasado, porque la historia hay que reescribirla en el medio.
2. **Reconstrucción completa desde silver** (gaps and islands): foto por (entidad, periodo), versión nueva cada vez que cambia la firma de atributos respecto al periodo anterior.

## Decisión

Opción 2. La dimensión tiene miles de filas y silver guarda toda la historia, así que reconstruirla cuesta segundos y siempre da el mismo resultado para la misma entrada.

```
periodo   firma                          cambio  version
202301    A|LIMA|...                     sí      1
202302    A|LIMA|...                     no      1
202307    A (REORGANIZADA)|LIMA|...      sí      2
```

`valido_desde` es el primer periodo de la versión, `valido_hasta` el inicio de la siguiente (exclusivo) o `999912` si está vigente. Los hechos resuelven la versión con un as-of join por `sec_ejec` y periodo.

## Consecuencias

- Recargar cualquier año, en cualquier orden, deja la historia correcta.
- Una recarga puede partir la historia en otros puntos y cambiar llaves que usan años no recargados. El pipeline detecta hechos que apuntan a versiones inexistentes y recalcula esos años (`_years_with_stale_keys`); un test cubre este caso.
