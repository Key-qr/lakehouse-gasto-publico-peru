-- Avance por sector con ranking dentro de cada año.
CREATE OR REPLACE TABLE marts.avance_sector AS
WITH base AS (
    SELECT
        ano_eje,
        sector_nombre                AS sector,
        nivel_gobierno_nombre        AS nivel_gobierno,
        SUM(monto_pim)               AS pim,
        SUM(monto_devengado)         AS devengado
    FROM marts.v_presupuesto_anual
    GROUP BY ALL
)
SELECT
    *,
    ROUND(devengado / NULLIF(pim, 0) * 100, 2)::DOUBLE                     AS avance_pct,
    RANK() OVER (PARTITION BY ano_eje ORDER BY pim DESC)                    AS ranking_pim,
    ROUND(pim / SUM(pim) OVER (PARTITION BY ano_eje) * 100, 2)::DOUBLE      AS participacion_pim_pct
FROM base;
