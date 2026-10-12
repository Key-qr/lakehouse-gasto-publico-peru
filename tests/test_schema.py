from gasto_lakehouse import schema


def test_contrato_tiene_las_63_columnas_del_diccionario():
    assert len(schema.SOURCE_COLUMNS) == 63
    assert len(set(schema.SOURCE_COLUMNS)) == 63


def test_grupos_de_columnas_son_disjuntos_y_cubren_todo():
    groups = [
        set(schema.PERIOD_COLUMNS),
        set(schema.CODE_COLUMNS),
        set(schema.NAME_COLUMNS),
        set(schema.AMOUNT_COLUMNS),
    ]
    assert sum(len(g) for g in groups) == 63
    assert set().union(*groups) == set(schema.COLUMNS)


def test_grano_no_incluye_montos_ni_nombres():
    assert not set(schema.GRAIN) & set(schema.AMOUNT_COLUMNS)
    assert not set(schema.GRAIN) & set(schema.NAME_COLUMNS)


def test_esquema_silver_cubre_todas_las_columnas():
    assert set(schema.SILVER_SCHEMA) == set(schema.COLUMNS)
