# ADR 0006 · PIA y PIM se tratan como stock, con detección automática

- Estado: aceptada
- Fecha: 2026-10

## Contexto

En el archivo del MEF cada fila tiene mes. Las fases de ejecución (certificado, comprometido, devengado, girado) son flujos del mes y se suman. El PIA y el PIM son presupuesto: si vienen repetidos en cada mes como foto acumulada, sumarlos multiplica el presupuesto por la cantidad de meses; si vienen solo en una fila por línea, hay que sumarlos.

El diccionario de datos oficial no aclara cuál de los dos casos aplica.

## Decisión

En `fact_presupuesto_anual` el PIA/PIM de cada línea presupuestal se agrega según un perfil que se calcula por año:

- en las líneas con más de un mes, si el PIM es distinto de cero en al menos el 80 % de los meses, se interpreta como stock y se toma el **último mes** de la línea (`latest`);
- si no, se interpreta como registro único y se **suma** (`sum`).

El modo usado queda en la columna `modo_presupuesto` del hecho y en el log. Se puede forzar con `LAKEHOUSE_BUDGET_SEMANTICS=latest|sum`.

## Consecuencias

- El avance (`devengado / PIM`) no queda inflado ni deflactado por una suposición sobre el formato.
- Si el MEF cambia el formato, el pipeline se adapta y el cambio queda registrado.
- Validación pendiente con datos reales: comparar el PIM total de un año contra el reporte publicado en Consulta Amigable.
