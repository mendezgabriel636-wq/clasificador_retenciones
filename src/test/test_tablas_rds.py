import pytest
import polars as pl
from datetime import date
from ..asignacion_retenciones.main_automatizacion_impositivas import (
    carga_base_retenciones,
    leer_base_rucs_sri,
    leer_ciiu_clasificado,
    leer_reglas_retencion_iva,
    leer_reglas_retencion_renta,
)


def test_leer_tablas():
    with pytest.raises(RuntimeError):
        leer_base_rucs_sri()

    with pytest.raises(RuntimeError):
        leer_ciiu_clasificado()

    with pytest.raises(RuntimeError):
        leer_reglas_retencion_iva()

    with pytest.raises(RuntimeError):
        leer_reglas_retencion_renta()


def test_carga_base_retenciones():
    df = pl.DataFrame(
        {
            "numero_ruc": [1234567890001],
            "numero_establecimiento_matriz": [1],
            "razon_social": ["TEST SA"],
            "estado_contribuyente": ["ACTIVO"],
            "clase_contribuyente": ["OTROS"],
            "obligado": [0],
            "agente_retencion": [0],
            "contribuyente_especial": [0],
            "contribuyente_fantasma": [0],
            "tipo_contribuyente": ["PERSONA NATURAL"],
            "codigo_ciiu": ["G4711"],
            "actividad_economica": ["COMERCIO AL POR MENOR"],
            "categoria": ["CATEGORIA TEST"],
            "nro_campo": [1],
            "porcentaje_retencion_renta": [1.75],
            "campo_formulario_103_ir": [310],
            "codigo_anexo_ir": [3101],
            "porcentaje_retencion_iva": [30.0],
            "campo_formulario_104_iva": [601],
            "codigo_anexo_iva": [6011],
            "fecha_carga": [date.today()],
            "fecha_inicio_actividades": [date(2010, 1, 1)],
            "fecha_actualizacion": [None],
            "fecha_suspension_definitiva": [None],
            "fecha_reinicio_actividades": [None],
        }
    )
    carga_base_retenciones(df)


def test_carga_base_retenciones_fecha_tipo_incorrecto():
    df = pl.DataFrame(
        {
            "numero_ruc": [1234567890001],
            "numero_establecimiento_matriz": [1],
            "razon_social": ["TEST SA"],
            "estado_contribuyente": ["ACTIVO"],
            "clase_contribuyente": ["OTROS"],
            "obligado": [0],
            "agente_retencion": [0],
            "contribuyente_especial": [0],
            "contribuyente_fantasma": [0],
            "tipo_contribuyente": ["PERSONA NATURAL"],
            "codigo_ciiu": ["G4711"],
            "actividad_economica": ["COMERCIO AL POR MENOR"],
            "categoria": ["CATEGORIA TEST"],
            "nro_campo": [1],
            "porcentaje_retencion_renta": [1.75],
            "campo_formulario_103_ir": [310],
            "codigo_anexo_ir": [3101],
            "porcentaje_retencion_iva": [30.0],
            "campo_formulario_104_iva": [601],
            "codigo_anexo_iva": [6011],
            "fecha_carga": ["no-soy-una-fecha"],
            "fecha_inicio_actividades": ["tampoco-soy-fecha"],
            "fecha_actualizacion": [None],
            "fecha_suspension_definitiva": [None],
            "fecha_reinicio_actividades": [None],
        }
    )
    with pytest.raises(Exception):
        carga_base_retenciones(df)


def test_carga_base_retenciones_falta_columna():
    df = pl.DataFrame(
        {
            "numero_ruc": [1234567890001],
            "numero_establecimiento_matriz": [1],
            "razon_social": ["TEST SA"],
            "estado_contribuyente": ["ACTIVO"],
            "clase_contribuyente": ["OTROS"],
            "obligado": [0],
            "agente_retencion": [0],
            "contribuyente_especial": [0],
            "contribuyente_fantasma": [0],
            "tipo_contribuyente": ["PERSONA NATURAL"],
            "codigo_ciiu": ["G4711"],
            "actividad_economica": ["COMERCIO AL POR MENOR"],
            "categoria": ["CATEGORIA TEST"],
            "nro_campo": [1],
            "porcentaje_retencion_renta": [1.75],
            "campo_formulario_103_ir": [310],
            "codigo_anexo_ir": [3101],
            "porcentaje_retencion_iva": [30.0],
            "campo_formulario_104_iva": [601],
            "codigo_anexo_iva": [6011],
            # fecha_carga ausente intencionalmente
            "fecha_inicio_actividades": [date(2010, 1, 1)],
            "fecha_actualizacion": [None],
            "fecha_suspension_definitiva": [None],
            "fecha_reinicio_actividades": [None],
        }
    )
    with pytest.raises(Exception):
        carga_base_retenciones(df)


def test_carga_base_retenciones_columna_extra():
    df = pl.DataFrame(
        {
            "numero_ruc": [1234567890001],
            "numero_establecimiento_matriz": [1],
            "razon_social": ["TEST SA"],
            "estado_contribuyente": ["ACTIVO"],
            "clase_contribuyente": ["OTROS"],
            "obligado": [0],
            "agente_retencion": [0],
            "contribuyente_especial": [0],
            "contribuyente_fantasma": [0],
            "tipo_contribuyente": ["PERSONA NATURAL"],
            "codigo_ciiu": ["G4711"],
            "actividad_economica": ["COMERCIO AL POR MENOR"],
            "categoria": ["CATEGORIA TEST"],
            "nro_campo": [1],
            "porcentaje_retencion_renta": [1.75],
            "campo_formulario_103_ir": [310],
            "codigo_anexo_ir": [3101],
            "porcentaje_retencion_iva": [30.0],
            "campo_formulario_104_iva": [601],
            "codigo_anexo_iva": [6011],
            "fecha_carga": [date.today()],
            "fecha_inicio_actividades": [date(2010, 1, 1)],
            "fecha_actualizacion": [None],
            "fecha_suspension_definitiva": [None],
            "fecha_reinicio_actividades": [None],
            "columna_extra": ["no_deberia_estar"],
        }
    )
    with pytest.raises(Exception):
        carga_base_retenciones(df)
