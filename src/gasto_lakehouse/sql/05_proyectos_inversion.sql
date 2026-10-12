-- Proyectos de inversión: presupuesto, ejecución y semáforo de avance.
CREATE OR REPLACE TABLE marts.proyectos_inversion AS
SELECT
    ano_eje,
    producto_proyecto                                  AS codigo_proyecto,
    ANY_VALUE(producto_proyecto_nombre)                AS proyecto,
    ANY_VALUE(ejecutora_nombre)                        AS entidad,
    ANY_VALUE(nivel_gobierno_nombre)                   AS nivel_gobierno,
    ANY_VALUE(departamento_meta_nombre)                AS departamento,
    SUM(monto_pim)                                     AS pim,
    SUM(monto_devengado)                               AS devengado,
    ROUND(SUM(monto_devengado) / NULLIF(SUM(monto_pim), 0) * 100, 2)::DOUBLE AS avance_pct,
    CASE
        WHEN SUM(monto_devengado) / NULLIF(SUM(monto_pim), 0) >= 0.75 THEN 'ALTO'
        WHEN SUM(monto_devengado) / NULLIF(SUM(monto_pim), 0) >= 0.40 THEN 'MEDIO'
        ELSE 'BAJO'
    END                                                AS semaforo
FROM marts.v_presupuesto_anual
WHERE tipo_act_proy_nombre = 'PROYECTO'
GROUP BY ano_eje, producto_proyecto;
