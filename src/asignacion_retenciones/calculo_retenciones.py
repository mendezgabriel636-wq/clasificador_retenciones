import polars as pl
from .retencion_iva import aplicar_retencion_iva
from .retencion_renta import aplicar_retencion_renta


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

    except Exception as e:
        raise RuntimeError(f"{e}")

    return aplicado_iva_renta
