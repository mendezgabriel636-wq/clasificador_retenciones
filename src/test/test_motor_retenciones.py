import pytest
import polars as pl

from asignacion_retenciones.calculo_retenciones import (
    aplicar_retencion_iva,
    aplicar_retencion_renta,
    formateo,
    calcular_retenciones,
)


# --- Casos de error: columnas faltantes o inexistentes ---

@pytest.mark.unit
def test_falta_columna_tabla_retencion_iva():
    tb = pl.DataFrame({"tipo_proveedor": ["A", "B"]})
    reglas = pl.DataFrame({"tipo_proveedor": ["A", "B"]})  # sin porcentaje_retencion_iva
    with pytest.raises(ValueError, match="porcentaje_retencion_iva"):
        aplicar_retencion_iva(tb, reglas)


@pytest.mark.unit
def test_columna_inexistente_en_tb_iva():
    tb = pl.DataFrame({"tipo_proveedor": ["A", "B"]})
    reglas = pl.DataFrame({
        "columna_inexistente": ["A", "B"],
        "porcentaje_retencion_iva": [10.0, 20.0],
    })
    with pytest.raises(ValueError, match="retencion_iva"):
        aplicar_retencion_iva(tb, reglas)


@pytest.mark.unit
def test_falta_columna_tabla_retencion_renta():
    tb = pl.DataFrame({"tipo_proveedor": ["A", "B"]})
    reglas = pl.DataFrame({"tipo_proveedor": ["A", "B"]})  # sin porcentaje_retencion_renta
    with pytest.raises(ValueError, match="porcentaje_retencion_renta"):
        aplicar_retencion_renta(tb, reglas)


@pytest.mark.unit
def test_columna_inexistente_en_tb_renta():
    tb = pl.DataFrame({"tipo_proveedor": ["A", "B"]})
    reglas = pl.DataFrame({
        "columna_inexistente": ["A", "B"],
        "porcentaje_retencion_renta": [10.0, 20.0],
    })
    with pytest.raises(ValueError, match="retencion_renta"):
        aplicar_retencion_renta(tb, reglas)


# --- Happy path: flujo exitoso ---

@pytest.mark.unit
def test_aplicar_retencion_iva_exitoso(df_base_rucs, df_reglas_iva):
    resultado = aplicar_retencion_iva(df_base_rucs, df_reglas_iva)
    assert "porcentaje_retencion_iva" in resultado.columns
    assert resultado.shape[0] == 1


@pytest.mark.unit
def test_aplicar_retencion_renta_exitoso(df_base_rucs, df_reglas_renta):
    resultado = aplicar_retencion_renta(df_base_rucs, df_reglas_renta)
    assert "porcentaje_retencion_renta" in resultado.columns
    assert resultado.shape[0] == 1


@pytest.mark.unit
def test_formateo_exitoso(df_resultado_valido):
    resultado = formateo(df_resultado_valido)
    assert resultado.shape == (1, 25)
    assert "porcentaje_retencion_iva" in resultado.columns
    assert "porcentaje_retencion_renta" in resultado.columns
    assert resultado["fecha_carga"].dtype == pl.Date


@pytest.mark.unit
def test_calcular_retenciones_flujo_completo(df_base_rucs, df_ciiu, df_reglas_iva, df_reglas_renta):
    resultado = calcular_retenciones(
        base_rucs_sri=df_base_rucs,
        ciiu_clasificado=df_ciiu,
        reglas_retenciones_iva=df_reglas_iva,
        reglas_retenciones_renta=df_reglas_renta,
    )
    assert resultado.shape == (1, 25)
    assert resultado["porcentaje_retencion_iva"][0] == pytest.approx(30.0, abs=0.01)
    assert resultado["porcentaje_retencion_renta"][0] == pytest.approx(1.75, abs=0.01)
