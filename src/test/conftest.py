import pytest
import polars as pl
from datetime import date


@pytest.fixture
def df_resultado_valido():
    """DataFrame completo con las 25 columnas y tipos esperados por formateo() y carga_base_retenciones()."""
    return pl.DataFrame({
        "numero_ruc": pl.Series([1234567890001], dtype=pl.Int64),
        "numero_establecimiento_matriz": pl.Series([1], dtype=pl.Int32),
        "razon_social": ["TEST SA"],
        "estado_contribuyente": ["ACTIVO"],
        "clase_contribuyente": ["OTROS"],
        "obligado": pl.Series([0], dtype=pl.Int8),
        "agente_retencion": pl.Series([0], dtype=pl.Int8),
        "contribuyente_especial": pl.Series([0], dtype=pl.Int8),
        "contribuyente_fantasma": pl.Series([0], dtype=pl.Int8),
        "tipo_contribuyente": ["PERSONA NATURAL"],
        "codigo_ciiu": ["G4711"],
        "actividad_economica": ["COMERCIO AL POR MENOR"],
        "categoria": ["CATEGORIA TEST"],
        "nro_campo": pl.Series([1], dtype=pl.Int32),
        "porcentaje_retencion_renta": pl.Series([1.75], dtype=pl.Float32),
        "campo_formulario_103_ir": pl.Series([310], dtype=pl.Int32),
        "codigo_anexo_ir": pl.Series([3101], dtype=pl.Int32),
        "porcentaje_retencion_iva": pl.Series([30.0], dtype=pl.Float32),
        "campo_formulario_104_iva": pl.Series([601.0], dtype=pl.Float32),
        "codigo_anexo_iva": pl.Series([6011], dtype=pl.Int32),
        "fecha_carga": pl.Series([date.today()], dtype=pl.Date),
        "fecha_inicio_actividades": pl.Series([date(2010, 1, 1)], dtype=pl.Date),
        "fecha_actualizacion": pl.Series([None], dtype=pl.Date),
        "fecha_suspension_definitiva": pl.Series([None], dtype=pl.Date),
        "fecha_reinicio_actividades": pl.Series([None], dtype=pl.Date),
    })


@pytest.fixture
def df_base_rucs():
    """Base de RUCs simulada — solo columnas que vienen de base_rucs_sri.
    Las columnas de retención (porcentajes, campos) las añade el join con las reglas.
    """
    return pl.DataFrame({
        "numero_ruc": pl.Series([1234567890001], dtype=pl.Int64),
        "numero_establecimiento_matriz": pl.Series([1], dtype=pl.Int32),
        "razon_social": ["TEST SA"],
        "estado_contribuyente": ["ACTIVO"],
        "clase_contribuyente": ["OTROS"],
        "obligado": pl.Series([0], dtype=pl.Int8),
        "agente_retencion": pl.Series([0], dtype=pl.Int8),
        "contribuyente_especial": pl.Series([0], dtype=pl.Int8),
        "contribuyente_fantasma": pl.Series([0], dtype=pl.Int8),
        "tipo_contribuyente": ["PERSONA NATURAL"],
        "codigo_ciiu": ["G4711"],
        "actividad_economica": ["COMERCIO AL POR MENOR"],
        "categoria": ["CATEGORIA TEST"],
        "fecha_carga": pl.Series([date.today()], dtype=pl.Date),
        "fecha_inicio_actividades": pl.Series([date(2010, 1, 1)], dtype=pl.Date),
        "fecha_actualizacion": pl.Series([None], dtype=pl.Date),
        "fecha_suspension_definitiva": pl.Series([None], dtype=pl.Date),
        "fecha_reinicio_actividades": pl.Series([None], dtype=pl.Date),
    })


@pytest.fixture
def df_ciiu():
    """Tabla CIIU clasificado mínima para el join con actividad_economica."""
    return pl.DataFrame({
        "actividad_economica": ["COMERCIO AL POR MENOR"],
        "tipo_concepto_iva": ["BIEN"],
        "tipo_concepto_ir": ["BIEN_MUEBLE"],
    })


@pytest.fixture
def df_reglas_iva():
    """Reglas IVA: claves de clasificación + columnas de salida.
    El join usa las claves que existan en tb; las columnas de salida se añaden al resultado.
    """
    return pl.DataFrame({
        "tipo_concepto_iva": ["BIEN"],
        "tipo_contribuyente": ["PERSONA NATURAL"],
        "clase_contribuyente": ["OTROS"],
        "categoria": ["CATEGORIA TEST"],
        "contribuyente_especial": pl.Series([0], dtype=pl.Int8),
        "porcentaje_retencion_iva": pl.Series([30.0], dtype=pl.Float32),
        "campo_formulario_104_iva": pl.Series([601], dtype=pl.Int32),
        "codigo_anexo_iva": pl.Series([6011], dtype=pl.Int32),
    })


@pytest.fixture
def df_reglas_renta():
    """Reglas renta: claves de clasificación + columnas de salida."""
    return pl.DataFrame({
        "tipo_concepto_ir": ["BIEN_MUEBLE"],
        "tipo_contribuyente": ["PERSONA NATURAL"],
        "clase_contribuyente": ["OTROS"],
        "categoria": ["CATEGORIA TEST"],
        "contribuyente_especial": pl.Series([0], dtype=pl.Int8),
        "nro_campo": pl.Series([1], dtype=pl.Int32),
        "porcentaje_retencion_renta": pl.Series([1.75], dtype=pl.Float32),
        "campo_formulario_103_ir": pl.Series([310], dtype=pl.Int32),
        "codigo_anexo_ir": pl.Series([3101], dtype=pl.Int32),
    })
