import pytest

from asignacion_retenciones.main_automatizacion_impositivas import (
    leer_actividades_economicas_faltantes,
    leer_base_rucs_catastro,
    leer_base_rucs_sri,
    leer_ciiu_clasificado,
    leer_ciiu_nivel6,
    leer_reglas_retencion_iva,
    leer_reglas_retencion_renta,
    leer_rucs_bendo,
)


# ---------------------------------------------------------------------------
# Tablas del catálogo DuckDB
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_leer_base_rucs_sri():
    df = leer_base_rucs_sri(limit=10)
    assert len(df) == 10
    assert {"numero_ruc", "numero_establecimiento_matriz", "actividad_economica",
            "tipo_contribuyente", "clase_contribuyente", "categoria",
            "contribuyente_especial", "agente_retencion",
            "obligado_llevar_contabilidad", "fecha_carga"}.issubset(set(df.columns))


@pytest.mark.integration
def test_leer_actividades_economicas_clasificadas():
    df = leer_ciiu_clasificado(limit=10)
    assert len(df) == 10
    assert {"actividad_economica", "codigo_ciiu",
            "tipo_concepto_iva", "tipo_concepto_ir"}.issubset(set(df.columns))


@pytest.mark.integration
def test_leer_base_rucs_catastro():
    df = leer_base_rucs_catastro(limit=10)
    assert len(df) == 10
    assert "numero_ruc" in df.columns


@pytest.mark.integration
def test_leer_ciiu_nivel6():
    df = leer_ciiu_nivel6(limit=10)
    assert len(df) == 10
    assert {"codigo", "descripcion"}.issubset(set(df.columns))


@pytest.mark.integration
def test_leer_actividades_economicas_faltantes():
    df = leer_actividades_economicas_faltantes(limit=10)
    assert len(df) == 10
    assert {"actividad_economica", "codigo_ciiu"}.issubset(set(df.columns))


@pytest.mark.integration
def test_leer_rucs_bendo():
    df = leer_rucs_bendo(limit=10)
    assert len(df) == 10
    assert "numero_ruc" in df.columns


# ---------------------------------------------------------------------------
# Tablas de reglas
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_leer_reglas_retencion_iva():
    df = leer_reglas_retencion_iva(limit=10)
    assert len(df) == 10
    assert {"tipo_concepto_iva", "porcentaje_retencion_iva",
            "campo_formulario_104_iva"}.issubset(set(df.columns))


@pytest.mark.integration
def test_leer_reglas_retencion_renta():
    df = leer_reglas_retencion_renta(limit=10)
    assert len(df) == 10
    assert {"tipo_concepto_ir", "porcentaje_retencion_renta",
            "campo_formulario_103_ir"}.issubset(set(df.columns))
