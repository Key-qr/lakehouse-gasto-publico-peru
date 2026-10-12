-- Vista ancha para exploración ad hoc: el hecho anual con todas sus dimensiones resueltas.
CREATE OR REPLACE VIEW marts.v_presupuesto_anual AS
SELECT
    f.ano_eje,
    e.sec_ejec,
    e.ejecutora_nombre,
    e.nivel_gobierno_nombre,
    e.sector_nombre,
    e.pliego_nombre,
    e.departamento_ejecutora_nombre,
    u.departamento_meta_nombre,
    fn.programa_ppto_nombre,
    fn.funcion_nombre,
    c.categoria_gasto_nombre,
    c.generica_nombre,
    fu.fuente_financiamiento_nombre,
    fu.rubro_nombre,
    p.tipo_act_proy_nombre,
    p.producto_proyecto,
    p.producto_proyecto_nombre,
    f.monto_pia,
    f.monto_pim,
    f.monto_certificado,
    f.monto_comprometido_anual,
    f.monto_devengado,
    f.monto_girado
FROM gold.fact_presupuesto_anual f
JOIN gold.dim_entidad e              USING (entidad_sk)
JOIN gold.dim_ubigeo u               USING (ubigeo_sk)
JOIN gold.dim_funcional fn           USING (funcional_sk)
JOIN gold.dim_clasificador_gasto c   USING (clasificador_gasto_sk)
JOIN gold.dim_fuente fu              USING (fuente_sk)
JOIN gold.dim_producto_proyecto p    USING (producto_proyecto_sk);
