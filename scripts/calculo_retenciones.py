from app.Negocio.qph.automatizacion_impositivas.retencion_iva import (
    aplicar_retencion_iva,
)
from app.Negocio.qph.automatizacion_impositivas.retencion_renta import (
    aplicar_retencion_renta,
)
from app.Negocio.qph.automatizacion_impositivas.procesamiento_base_rucs_sri import (
    procesamiento,
)
import polars as pl
import time  # CAMBIO: Agregado para fecha_carga
from sqlalchemy.engine import Engine


def calcular_retenciones(engine_data_fact: Engine) -> pl.DataFrame:
    # =====================
    # 1. Obtener base corregida y clasificada
    # =====================
    base_rucs_corregida_clasificada = procesamiento(engine_data_fact=engine_data_fact)

    # =====================
    # 2. Aplicar retenciones IVA y Renta por separado
    # =====================
    aplicado_iva = aplicar_retencion_iva(base_rucs_corregida_clasificada)
    aplicado_renta = aplicar_retencion_renta(base_rucs_corregida_clasificada)

    # =====================
    # 3. Unir resultados
    # =====================
    columnas_renta = [
        "numero_ruc",
        "codigo_sri_renta",
        "porcentaje_renta",
        "descripcion_renta",
        "base_calculo_renta",
    ]

    # =========================================================================
    # CAMBIO: Se ampliaron las columnas en el select para incluir las columnas
    #         extra de SRI necesarias para el formato RDS
    #         NOTA: estado_establecimiento ya viene del select base
    # =========================================================================
    iva_renta = aplicado_iva.join(
        aplicado_renta.select(columnas_renta), on="numero_ruc", how="left"
    ).select(
        [
            "numero_ruc",
            "razon_social",
            "estado_contribuyente",
            "tipo_contribuyente",
            "clase_contribuyente",
            "categoria",
            "obligado_llevar_contabilidad",
            "agente_retencion",
            "contribuyente_especial",
            "CODIGO",
            "actividad_economica",
            "estado_establecimiento",
            # IVA
            "porcentaje_retencion_iva",
            "motivo_iva",
            # Renta
            "codigo_sri_renta",
            "porcentaje_renta",
            "base_calculo_renta",
            # CAMBIO: Columnas extra de SRI para formato RDS
            "nombre_fantasia_comercial",
            # estado_establecimiento ya incluido arriba
            "numero_establecimiento",
            "id_establecimiento",
            "fecha_inicio_actividades_comercio",
            "fecha_actualizacion_comercio",
            "fecha_cese_comercio",
            "fecha_reinicio_actividades_comercio",
            "direccion_completa",
            "motivo_cancelacion_suspension",
            "contribuyente_fantasma",
            "transacciones_inexistente",
            "nombre_representante_legal",
            "identificacion_representante_legal",
            "representantes_legales",
        ]
    )

    # =====================
    # 4. Mapeo campo formulario 104 IVA
    # =====================
    campo_formulario_iva_map = {10: 721, 20: 723, 30: 725, 50: 727, 70: 729, 100: 731}

    iva_renta = iva_renta.with_columns(
        pl.col("porcentaje_retencion_iva")
        .replace(campo_formulario_iva_map, default=0)
        .alias("campo_formulario_104_iva")
    )

    # =====================
    # 5. Cruce con tabla de retenciones → campo formulario 103 IR
    # =====================
    query_tabla_retenciones = """
    SELECT *
    FROM tabla_retenciones
    """
    try:
        _raw_tabla = pl.read_database(query_tabla_retenciones, connection=engine_data_fact)
    except Exception as e:
        raise RuntimeError(f"[calcular_retenciones] Fallo al leer 'tabla_retenciones': {e}") from e

    try:
        tabla_retenciones = _raw_tabla.select(
            [
                pl.col(r"casillero Formulario 103  base imponible").alias(
                    "campo_formulario_103_ir"
                ),
                pl.col("CÃ³digo del Anexo .1").cast(pl.Utf8).alias("codigo_anexo_ir"),
            ]
        )
    except Exception as e:
        raise RuntimeError(
            f"[calcular_retenciones] Fallo al seleccionar columnas de 'tabla_retenciones' — "
            f"verifique nombres y encoding. Columnas disponibles: {_raw_tabla.columns}. Error: {e}"
        ) from e

    iva_renta_final = iva_renta.join(
        tabla_retenciones,
        left_on="codigo_sri_renta",
        right_on="codigo_anexo_ir",
        how="left",
    ).rename({"codigo_sri_renta": "codigo_anexo_ir"})

    # =========================================================================
    # CAMBIO: Formateo final para compatibilidad con esquema RDS
    # =========================================================================
    iva_renta_final = formatear_para_rds(iva_renta_final)

    return iva_renta_final


# =============================================================================
# CAMBIO: Nueva función — Aplica todos los renombramientos, derivaciones,
#         constantes, casteos y limpieza que requiere el esquema RDS
# =============================================================================
def formatear_para_rds(df: pl.DataFrame) -> pl.DataFrame:

    # ----- RENOMBRAMIENTOS -----
    df = df.rename(
        {
            "obligado_llevar_contabilidad": "obligado",
            "CODIGO": "codigo_ciiu",
            "porcentaje_renta": "porcentaje_retencion_renta",
            "fecha_inicio_actividades_comercio": "fecha_inicio_actividades",
            "fecha_actualizacion_comercio": "fecha_actualizacion",
            "fecha_cese_comercio": "fecha_suspension_definitiva",
            "fecha_reinicio_actividades_comercio": "fecha_reinicio_actividades",
        }
    )

    # ----- DERIVACIONES -----
    df = df.with_columns(
        [
            # numero_ruc_str: RUC como string, sin .0, rellenado a 13 dígitos
            pl.col("numero_ruc")
            .cast(pl.Utf8)
            .str.replace(r"\.0$", "")
            .str.zfill(13)
            .alias("numero_ruc_str"),
            # nombre_comercial: copia de nombre_fantasia_comercial
            pl.col("nombre_fantasia_comercial").alias("nombre_comercial"),
            # Split de direccion_completa → provincia, cantón, parroquia
            pl.col("direccion_completa")
            .str.split_exact(" / ", 3)
            .struct.field("field_0")
            .alias("descripcion_provincia_est"),
            pl.col("direccion_completa")
            .str.split_exact(" / ", 3)
            .struct.field("field_1")
            .alias("descripcion_canton_est"),
            pl.col("direccion_completa")
            .str.split_exact(" / ", 3)
            .struct.field("field_2")
            .alias("descripcion_parroquia_est"),
            # codigo_anexo_iva: mapeo desde porcentaje_retencion_iva
            pl.col("porcentaje_retencion_iva")
            .replace({10: 9, 20: 10, 30: 1, 50: 11, 70: 2, 100: 3}, default=0)
            .alias("codigo_anexo_iva"),
            # Constantes NaN
            pl.lit(None).cast(pl.Utf8).alias("provincia_jurisdiccion"),
            pl.lit(None).cast(pl.Utf8).alias("provincia_archivo_procesamiento"),
            pl.lit(None).cast(pl.Utf8).alias("nro_campo"),
            # fecha_carga: fecha actual
            pl.lit(time.strftime("%Y-%m-%d")).alias("fecha_carga"),
        ]
    )

    # cedula y cedula_str: derivadas de numero_ruc sin '001' al final
    df = df.with_columns(
        [
            pl.col("numero_ruc").cast(pl.Utf8).str.replace(r"001$", "").alias("cedula"),
            pl.col("numero_ruc_str").str.replace(r"001$", "").alias("cedula_str"),
        ]
    )

    # ----- CASTEOS A STRING (RDS los espera como String) -----
    df = df.with_columns(
        [
            pl.col("porcentaje_retencion_renta").cast(pl.Utf8),
            pl.col("porcentaje_retencion_iva").cast(pl.Utf8),
            pl.col("campo_formulario_104_iva").cast(pl.Utf8),
            pl.col("campo_formulario_103_ir").cast(pl.Utf8),
            pl.col("codigo_anexo_iva").cast(pl.Utf8),
        ]
    )

    # ----- CASTEOS A NUMÉRICO (RDS los espera como TINYINT 0/1) -----
    df = df.with_columns(
        [
            pl.when(
                pl.col("obligado")
                .cast(pl.Utf8)
                .str.to_uppercase()
                .is_in(["SI", "SÍ", "1", "TRUE", "S"])
            )
            .then(1)
            .otherwise(0)
            .cast(pl.Int8)
            .alias("obligado"),
            pl.col("agente_retencion")
            .cast(pl.Utf8)
            .str.to_uppercase()
            .map_elements(
                lambda v: 1 if v in ("SI", "SÍ", "1", "TRUE", "S") else 0,
                return_dtype=pl.Int8,
            )
            .alias("agente_retencion"),
            pl.col("contribuyente_especial")
            .cast(pl.Utf8)
            .str.to_uppercase()
            .map_elements(
                lambda v: 1 if v in ("SI", "SÍ", "1", "TRUE", "S") else 0,
                return_dtype=pl.Int8,
            )
            .alias("contribuyente_especial"),
            pl.col("contribuyente_fantasma")
            .cast(pl.Utf8)
            .str.to_uppercase()
            .map_elements(
                lambda v: 1 if v in ("SI", "SÍ", "1", "TRUE", "S") else 0,
                return_dtype=pl.Int8,
            )
            .alias("contribuyente_fantasma"),
            pl.col("transacciones_inexistente")
            .cast(pl.Utf8)
            .str.to_uppercase()
            .map_elements(
                lambda v: 1 if v in ("SI", "SÍ", "1", "TRUE", "S") else 0,
                return_dtype=pl.Int8,
            )
            .alias("transacciones_inexistente"),
        ]
    )

    # ----- DESCARTAR columnas que no están en el esquema RDS -----
    df = df.drop(["motivo_iva", "base_calculo_renta", "direccion_completa"])

    return df
