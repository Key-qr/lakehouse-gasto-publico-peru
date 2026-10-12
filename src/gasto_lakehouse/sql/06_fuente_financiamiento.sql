-- Composición del presupuesto por fuente de financiamiento.
CREATE OR REPLACE TABLE marts.fuente_financiamiento AS
SELECT
    ano_eje,
    fuente_financiamiento_nombre                       AS fuente,
    rubro_nombre                                       AS rubro,
    SUM(monto_pim)                                     AS pim,
    SUM(monto_devengado)                               AS devengado,
    ROUND(SUM(monto_devengado) / NULLIF(SUM(monto_pim), 0) * 100, 2)::DOUBLE AS avance_pct
FROM marts.v_presupuesto_anual
GROUP BY ALL;
