-- Avance de ejecución por departamento donde se ejecuta el gasto y nivel de gobierno.
CREATE OR REPLACE TABLE marts.avance_departamento AS
SELECT
    ano_eje,
    departamento_meta_nombre                         AS departamento,
    nivel_gobierno_nombre                            AS nivel_gobierno,
    SUM(monto_pia)                                   AS pia,
    SUM(monto_pim)                                   AS pim,
    SUM(monto_devengado)                             AS devengado,
    SUM(monto_girado)                                AS girado,
    ROUND(SUM(monto_devengado) / NULLIF(SUM(monto_pim), 0) * 100, 2)::DOUBLE AS avance_pct
FROM marts.v_presupuesto_anual
GROUP BY ALL;
