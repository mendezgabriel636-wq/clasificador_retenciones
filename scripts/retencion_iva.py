"""
Motor de retención IVA — Bendo (pasarela de pagos)
Normativa:
  - NAC-DGERCGC20-00000061 + reformas (porcentajes retención IVA)
  - Art.97.9 LRTI (RIMPE Negocio Popular: IVA 0%)
  - Art.97.10 LRTI reformado por Art.24 Ley Eficiencia Económica
    (R.O. 461-S, 20-dic-2023): se ELIMINA exención pago electrónico
    para RIMPE Emprendedor → retención normal
Vigente a abril 2026
"""
from typing import Optional, Dict
import math
import polars as pl

_EXCEPCIONES_ART3 = {
    3.1: "Contribuyente Especial (excepción general)",
    3.2: "Instituciones del Estado",
    3.3: "Empresas públicas (LOEP)",
    3.4: "Compañías de aviación",
    3.5: "Agencias de viaje (solo pasajes aéreos)",
    3.6: "Distribuidores combustible (solo derivados petróleo)",
    3.7: "Instituciones del sistema financiero (serv. financieros)",
    3.8: "Emisoras de tarjetas de crédito (comisiones)",
    3.9: "Voceadores/distribuidores periódicos y revistas",
    3.10: "Exportadores habituales (agentes retención o CE)",
    3.11: "Sociedades asociación público-privada",
}


def calcular_porcentaje_retencion_iva(
    tipo_contribuyente: str,        # "PERSONAS NATURALES" o "SOCIEDADES"
    clase_contribuyente: str,       # "ESPECIAL", "OTROS", "RIMPE"
    categoria: Optional[str],       # "EMPRENDEDOR", "NEGOCIO POPULAR", None
    obligado_contabilidad: bool,
    contribuyente_especial: bool,
    tipo: str,                      # "BIEN", "SERVICIO", "SERVICIO_PROFESIONAL", "CONSTRUCCION", "ARRIENDO_INMUEBLE"
    excepcion_art3: Optional[float] # 3.2, 3.4, 3.5, ... o None/NaN
) -> Dict[str, any]:

    # =========================================================================
    # NORMALIZAR INPUTS
    # =========================================================================
    tipo_contrib = str(tipo_contribuyente).upper().strip() if tipo_contribuyente else ""
    es_sociedad = "SOCIEDAD" in tipo_contrib
    if not es_sociedad and "NATURAL" not in tipo_contrib:
        es_sociedad = True

    clase = str(clase_contribuyente).upper().strip() if clase_contribuyente else ""
    es_rimpe = clase == "RIMPE"

    def to_bool(val):
        if val is None:
            return False
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return val == 1
        return str(val).upper().strip() in ('SI', 'SÍ', 'YES', '1', 'TRUE', 'S')

    es_ce = to_bool(contribuyente_especial) or clase == "ESPECIAL"

    cat = str(categoria).upper().strip() if categoria else ""
    es_negocio_popular = "NEGOCIO POPULAR" in cat

    obligado = to_bool(obligado_contabilidad)
    tipo_op = str(tipo).upper().strip() if tipo else "SERVICIO"

    exc = None
    if excepcion_art3 is not None:
        try:
            v = float(excepcion_art3)
            if not math.isnan(v):
                exc = v
        except (ValueError, TypeError):
            pass

    def resultado(pct: int, motivo: str, art: str):
        return {"porcentaje": pct, "articulo": art, "motivo": motivo}

    # =========================================================================
    # PASO 1: EXCEPCIONES ART.3
    # =========================================================================
    if exc is not None:
        desc = _EXCEPCIONES_ART3.get(exc, f"Excepción Art.3 num.{exc}")
        return resultado(0, desc, f"Art.3.{str(exc).replace('.0','')}")

    # =========================================================================
    # PASO 2: RIMPE NEGOCIO POPULAR — No desglosa IVA
    # =========================================================================
    if es_rimpe and es_negocio_popular:
        return resultado(0, "RIMPE Negocio Popular - no desglosa IVA", "Art.97.9 LRTI")

    # =========================================================================
    # PASO 3: RIMPE EMPRENDEDOR — Retención normal (régimen general)
    # Art.97.10 LRTI reformado por Art.24 Ley Eficiencia Económica
    # (R.O. 461-S, 20-dic-2023): se eliminó la exención por pago
    # electrónico. Todo pago a emprendedor RIMPE está sujeto a
    # retención. Las retenciones constituyen crédito tributario.
    # Porcentajes: los de Res. NAC-DGERCGC20-00000061 (30%/70%/100%)
    # =========================================================================
    if es_rimpe and not es_negocio_popular:
        if tipo_op == "CONSTRUCCION":
            return resultado(30, "RIMPE Emprendedor - Construcción", "Art.7 Res.61")
        elif tipo_op == "BIEN":
            return resultado(30, "RIMPE Emprendedor - Bien", "Art.4.a Res.61")
        elif tipo_op == "SERVICIO_PROFESIONAL":
            return resultado(100, "RIMPE Emprendedor - Profesional", "Art.4.c.i Res.61")
        elif tipo_op == "ARRIENDO_INMUEBLE":
            return resultado(100, "RIMPE Emprendedor - Arriendo", "Art.4.c.ii Res.61")
        else:
            return resultado(70, "RIMPE Emprendedor - Servicio", "Art.4.b Res.61")

    # =========================================================================
    # PASO 4: CONSTRUCCIÓN — Siempre 30% (Art.7)
    # =========================================================================
    if tipo_op == "CONSTRUCCION":
        return resultado(30, "Construcción", "Art.7 Res.61")

    # =========================================================================
    # PASO 5: CONTRIBUYENTE ESPECIAL — 10%/20% (Art.5)
    # =========================================================================
    if es_ce:
        if tipo_op == "BIEN":
            return resultado(10, "CE - Bien", "Art.5.a Res.61")
        return resultado(20, "CE - Servicio/Derecho", "Art.5.b Res.61")

    # =========================================================================
    # PASO 6: RÉGIMEN GENERAL (Art.4)
    # =========================================================================
    if es_sociedad:
        if tipo_op == "BIEN":
            return resultado(30, "Sociedad - Bien", "Art.4.a Res.61")
        return resultado(70, "Sociedad - Servicio", "Art.4.b Res.61")

    # Persona Natural
    if tipo_op == "SERVICIO_PROFESIONAL":
        return resultado(100, "PN - Servicio profesional", "Art.4.c.i Res.61")

    if tipo_op == "ARRIENDO_INMUEBLE" and not obligado:
        return resultado(100, "PN no obligada - Arriendo inmueble", "Art.4.c.ii Res.61")

    if tipo_op == "BIEN":
        return resultado(30, "PN - Bien", "Art.4.a Res.61")

    return resultado(70, "PN - Servicio", "Art.4.b Res.61")


def aplicar_retencion_iva(df: pl.DataFrame) -> pl.DataFrame:
    columnas = [
        'tipo_contribuyente', 'clase_contribuyente', 'categoria',
        'obligado_llevar_contabilidad', 'contribuyente_especial',
        'tipo_concepto_iva', 'excepcion_art3'
    ]

    resultados = (
        df.with_columns(
            pl.struct(columnas)
            .map_elements(lambda row: calcular_porcentaje_retencion_iva(
                tipo_contribuyente=row['tipo_contribuyente'],
                clase_contribuyente=row['clase_contribuyente'],
                categoria=row['categoria'],
                obligado_contabilidad=row['obligado_llevar_contabilidad'],
                contribuyente_especial=row['contribuyente_especial'],
                tipo=row['tipo_concepto_iva'],
                excepcion_art3=row['excepcion_art3']
            ), return_dtype=pl.Struct({"porcentaje": pl.Int64, "articulo": pl.Utf8, "motivo": pl.Utf8}))
            .alias('resultado_iva')
        )
        .with_columns([
            pl.col('resultado_iva').struct.field('porcentaje').alias('porcentaje_retencion_iva'),
            pl.col('resultado_iva').struct.field('articulo').alias('articulo_iva'),
            pl.col('resultado_iva').struct.field('motivo').alias('motivo_iva'),
        ])
        .drop('resultado_iva')
    )

    return resultados
