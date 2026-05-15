import polars as pl
from .retencion_iva import aplicar_retencion_iva
from .retencion_renta import aplicar_retencion_renta


def formateo(tb: pl.DataFrame) -> pl.DataFrame:
    columnas_tipos = {
        "numero_ruc": pl.Int64,
        "numero_establecimiento_matriz": pl.Int32,
        "razon_social": pl.Utf8,
        "estado_contribuyente": pl.Utf8,
        "clase_contribuyente": pl.Utf8,
        "obligado": pl.Int8,
        "agente_retencion": pl.Int8,
        "contribuyente_especial": pl.Int8,
        "contribuyente_fantasma": pl.Int8,
        "tipo_contribuyente": pl.Utf8,
        "codigo_ciiu": pl.Utf8,
        "actividad_economica": pl.Utf8,
        "categoria": pl.Utf8,
        "nro_campo": pl.Int32,
        "porcentaje_retencion_renta": pl.Float32,
        "campo_formulario_103_ir": pl.Int32,
        "codigo_anexo_ir": pl.Int32,
        "porcentaje_retencion_iva": pl.Float32,
        "campo_formulario_104_iva": pl.Float32,
        "codigo_anexo_iva": pl.Int32,
        "fecha_carga": pl.Date,
        "fecha_inicio_actividades": pl.Date,
        "fecha_actualizacion": pl.Date,
        "fecha_suspension_definitiva": pl.Date,
        "fecha_reinicio_actividades": pl.Date,
    }
    columnas = columnas_tipos.keys()
    try:
        tb_formateada = tb.select(columnas).cast(pl.Schema(columnas_tipos))
    except Exception as e:
        raise RuntimeError(f"[calcular_retenciones.formateo_tabla_final] {e}")
    return tb_formateada


def calcular_retenciones(
    base_rucs_sri: pl.DataFrame, ciiu_clasificado: pl.DataFrame
) -> pl.DataFrame:
    try:
        base_rucs_corregida_clasificada = base_rucs_sri.join(
            ciiu_clasificado, on=["actividad_economica"], how="left"
        )
    except Exception as e:
        raise RuntimeError(f"[calcular_retenciones.asignar_metadatos] {e}")

    try:
        aplicado_iva = aplicar_retencion_iva(base_rucs_corregida_clasificada)
        aplicado_iva_renta = aplicar_retencion_renta(aplicado_iva)
        tabla_resultado = formateo(aplicado_iva_renta)
    except Exception as e:
        raise RuntimeError(f"{e}")

    return tabla_resultado
