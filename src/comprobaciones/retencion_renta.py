# =============================================================================
# REGLAS DE RETENCIÓN DE RENTA - BENDO ECUADOR
# =============================================================================
# Resolución NAC-DGERCGC26-00000009 (vigente desde 01-marzo-2026)
#
# ENTRADA: Tabla de proveedores con las siguientes columnas:
#   - ruc
#   - tipo_contribuyente (PERSONA NATURAL / SOCIEDAD)
#   - clase_contribuyente (ESPECIAL / OTROS / RIMPE)
#   - categoria (EMPRENDEDOR / NEGOCIO POPULAR / REGIMEN GENERAL)
#   - cod_ciiu / codigo_ciiu
#   - tipo_concepto_ir (ya clasificado desde tabla CIIU)
#   - obligado_llevar_contabilidad (SI / NO)
#   - agente_retencion (SI / NO)
#   - contribuyente_especial (SI / NO)
#
# SALIDA: codigo_sri_renta, porcentaje_renta, descripcion_renta
# =============================================================================

import polars as pl
from typing import Tuple, Optional, Dict

# =============================================================================
# TABLA: CONCEPTOS_RETENCION_SRI (84 códigos residentes)
# ACTUALIZAR cuando cambie la normativa
# =============================================================================

#=====================================
#SELECT *
#FROM df_conceptos_sri_cod_prcj_desc
#==============================


# =============================================================================
# MAPEO: tipo_concepto_ir → codigo_sri
# Incluye diferenciación por tipo_contribuyente (PN vs SOC)
# =============================================================================


def obtener_codigo_sri(tipo_concepto_ir: str, tipo_contribuyente: str) -> str:
    """
    Dado un tipo_concepto_ir y tipo_contribuyente, retorna el codigo_sri.

    Parámetros:
    - tipo_concepto_ir: valor de la columna tipo_concepto_ir de la tabla
    - tipo_contribuyente: "PERSONA NATURAL" o "SOCIEDAD"

    Retorna:
    - codigo_sri: código oficial del SRI
    """
    # Normalizar
    tipo_concepto = str(tipo_concepto_ir).upper().strip() if tipo_concepto_ir else ""
    tipo_contrib = str(tipo_contribuyente).upper().strip() if tipo_contribuyente else ""

    es_pn = "NATURAL" in tipo_contrib or tipo_contrib == "PN"

    # Mapeo tipo_concepto → codigo_sri
    # Para conceptos con diferencia PN/SOC, se usa condicional

    # =====================================================
    # SELECT *
    # FROM df_conceptos_a_formulario_104
    # =====================================================
    # SELECT *
    # FROM df_codigos_retencion_tipo_beneficiario
    # =====================================================

    conceptos = list(df_codigos_retencion_tipo_beneficiario['concepto'])
    
    if tipo_concepto in conceptos:
        valores_concepto = df_codigos_retencion_tipo_beneficiario.loc[df_codigos_retencion_tipo_beneficiario['concepto']== tipo_concepto,['codigo_pn', 'codigo_soc']].values
        codigo_pn = valores_concepto[0][0]
        codigo_soc = valores_concepto[0][1]
        return codigo_pn if es_pn else codigo_soc
    
    tipos_fromulario_104 = list(df_conceptos_a_formulario_104['tipo'])
    # Buscar en mapeo simple
    if tipo_concepto in tipos_fromulario_104:
        if pn:
            columna = "codigo_pn"
        else: 
            columna = "codigo_no_pn"
        return df_conceptos_a_formulario_104.loc[df_conceptos_a_formulario_104['tipo']==tipo_concepto,f'{columna}'].values[0]

    # Si no encuentra, retornar residual
    return "3440"


# =============================================================================
# FUNCIÓN PRINCIPAL: calcular_retencion_renta
# =============================================================================


def calcular_retencion_renta(row: Dict) -> Tuple[str, str, str, str]:
    """
    Calcula la retención de renta para una fila de la tabla de proveedores.

    Parámetros:
    - row: dict con las columnas del proveedor (compatible con pl.struct map_elements)

    Retorna:
    - Tupla: (codigo_sri, porcentaje_str, descripcion, base_calculo)
    # CAMBIO: porcentaje se retorna como str() para compatibilidad con pl.List(pl.String)
    """

    # Extraer valores de la fila (normalizar)
    contribuyente_especial = str(row.get("contribuyente_especial", "")).upper().strip()
    clase_contribuyente = str(row.get("clase_contribuyente", "")).upper().strip()
    categoria = str(row.get("categoria", "")).upper().strip()
    tipo_contribuyente = str(row.get("tipo_contribuyente", "")).upper().strip()
    tipo_concepto_ir = str(row.get("tipo_concepto_ir", "")).upper().strip()

    # =========================================================================
    # REGLA 1: ¿Es Contribuyente Especial?
    # =========================================================================
    es_contribuyente_especial = contribuyente_especial in [
        "1",
        "1.0",
        "SI",
        "SÍ",
        "S",
        "TRUE",
        "VERDADERO",
    ] or (
        isinstance(row.get("contribuyente_especial"), (int, float))
        and row.get("contribuyente_especial") == 1
    )

    # CAMBIO: str(0.0) en vez de 0.0
    if es_contribuyente_especial:
        return ("332", str(0.0), "NO RETENER - Contribuyente Especial", "Art.92 LORTI")

    # CAMBIO: str(0.0) en vez de 0.0
    if clase_contribuyente == "ESPECIAL":
        return ("332", str(0.0), "NO RETENER - Contribuyente Especial", "Art.92 LORTI")

    # =========================================================================
    # REGLA 2: ¿Es RIMPE Negocio Popular?
    # =========================================================================
    es_rimpe = clase_contribuyente == "RIMPE" or "RIMPE" in clase_contribuyente
    es_negocio_popular = "NEGOCIO" in categoria and "POPULAR" in categoria

    if es_rimpe and es_negocio_popular:
        concepto = df_conceptos_sri_cod_prcj_desc[df_conceptos_sri_cod_prcj_desc["codigo"]== "332"]
        # CAMBIO: str(concepto['porcentaje'])
        return (
            "332",
            str(concepto["porcentaje"].values[0]),
            concepto["descripcion"].values[0],
            "RIMPE Negocio Popular",
        )

    # =========================================================================
    # REGLA 3: ¿Es RIMPE Emprendedor?
    # =========================================================================
    es_emprendedor = "EMPRENDEDOR" in categoria

    if es_rimpe and es_emprendedor:
        concepto = df_conceptos_sri_cod_prcj_desc[df_conceptos_sri_cod_prcj_desc["codigo"]== "343"]
        # CAMBIO: str(concepto['porcentaje'])
        return (
            "343",
            str(concepto["porcentaje"].values[0]),
            concepto["descripcion"].values[0],
            "RIMPE Emprendedor",
        )

    # =========================================================================
    # REGLA 4: Clasificar por tipo_concepto_ir
    # =========================================================================
    codigo_sri = obtener_codigo_sri(tipo_concepto_ir, tipo_contribuyente)
    

    if codigo_sri in list(df_conceptos_sri_cod_prcj_desc['codigo']):
        concepto = df_conceptos_sri_cod_prcj_desc[df_conceptos_sri_cod_prcj_desc['codigo']==codigo_sri]
        # CAMBIO: str(concepto['porcentaje'])
        return (
            codigo_sri,
            str(concepto["porcentaje"].values[0]),
            concepto["descripcion"].values[0],
            f"{tipo_concepto_ir} → {codigo_sri}",
        )

    # Fallback: residual 3%
    concepto = df_conceptos_sri_cod_prcj_desc[df_conceptos_sri_cod_prcj_desc["codigo"]== "3440"]
    # CAMBIO: str(concepto['porcentaje'])
    return ("3440", str(concepto["porcentaje"].values[0]), concepto["descripcion"].values[0], "Residual")


# =============================================================================
# FUNCIÓN: Aplicar a DataFrame completo
# =============================================================================


def aplicar_retencion_renta(df: pl.DataFrame) -> pl.DataFrame:
    """
    Aplica el cálculo de retención de renta a todo el DataFrame.

    Parámetros:
    - df: DataFrame Polars con las columnas de proveedores

    Retorna:
    - DataFrame con columnas adicionales:
        - codigo_sri_renta
        - porcentaje_renta
        - descripcion_renta
        - base_calculo_renta
    """
    columnas = [
        "tipo_contribuyente",
        "clase_contribuyente",
        "categoria",
        "contribuyente_especial",
        "tipo_concepto_ir",
    ]

    resultado_basico = (
        df.with_columns(
            pl.struct(columnas)
            .map_elements(calcular_retencion_renta, return_dtype=pl.List(pl.String))
            .alias("resultado_renta")
        )
        .with_columns(
            [
                pl.col("resultado_renta").list.get(0).alias("codigo_sri_renta"),
                pl.col("resultado_renta")
                .list.get(1)
                .cast(pl.Float64)
                .alias("porcentaje_renta"),
                pl.col("resultado_renta").list.get(2).alias("descripcion_renta"),
                pl.col("resultado_renta").list.get(3).alias("base_calculo_renta"),
            ]
        )
        .drop("resultado_renta")
    )

    df_modificado = df.filter(
        (pl.col("tipo_concepto_iva") == "BIEN")
        & (pl.col("probabilidad_vende_servicios") == "ALTA")
    ).with_columns(pl.lit("SERVICIO_MANO_OBRA").alias("tipo_concepto_ir"))

    resultado_modificado = (
        df_modificado.with_columns(
            pl.struct(columnas)
            .map_elements(calcular_retencion_renta, return_dtype=pl.List(pl.String))
            .alias("resultado_renta")
        )
        .with_columns(
            [
                pl.col("resultado_renta")
                .list.get(0)
                .alias("codigo_sri_renta_modificado"),
                pl.col("resultado_renta")
                .list.get(1)
                .cast(pl.Float64)
                .alias("porcentaje_renta_modificado"),
                pl.col("resultado_renta")
                .list.get(2)
                .alias("descripcion_renta_modificado"),
                pl.col("resultado_renta")
                .list.get(3)
                .alias("base_calculo_renta_modificado"),
            ]
        )
        .drop("resultado_renta")
        .select(
            [
                "numero_ruc",
                "codigo_sri_renta_modificado",
                "descripcion_renta_modificado",
                "base_calculo_renta_modificado",
                "porcentaje_renta_modificado",
                "probabilidad_vende_servicios",
            ]
        )
    )

    resultado = resultado_basico.join(resultado_modificado, on="numero_ruc", how="left")

    return resultado
