-- Curva mensual de ejecución: devengado del mes, acumulado y % de avance sobre el PIM del año.
CREATE OR REPLACE TABLE marts.curva_ejecucion AS
WITH mensual AS (
    SELECT
        t.ano,
        t.mes,
        t.nombre_mes,
        SUM(f.monto_devengado)  AS devengado_mes,
        SUM(f.monto_girado)     AS girado_mes
    FROM gold.fact_ejecucion_mensual f
    JOIN gold.dim_tiempo t USING (periodo_sk)
    GROUP BY ALL
),
pim AS (
    SELECT ano_eje AS ano, SUM(monto_pim) AS pim_anual
    FROM gold.fact_presupuesto_anual
    GROUP BY ALL
)
SELECT
    m.*,
    SUM(m.devengado_mes) OVER (PARTITION BY m.ano ORDER BY m.mes)            AS devengado_acumulado,
    p.pim_anual,
    ROUND(
        SUM(m.devengado_mes) OVER (PARTITION BY m.ano ORDER BY m.mes) / NULLIF(p.pim_anual, 0) * 100,
        2
    )::DOUBLE                                                                 AS avance_acumulado_pct
FROM mensual m
JOIN pim p USING (ano)
ORDER BY m.ano, m.mes;
