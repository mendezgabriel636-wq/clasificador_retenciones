"""
================================================================================
MOTOR SIMPLIFICADO DE RETENCIÓN RENTA - ECUADOR SRI
================================================================================

Combina:
  - Tasas y códigos SRI de reglas_retencion_renta.py (NAC-DGERCGC26-00000009)
  - Motor vectorizado DuckDB con prioridades
  - Contribuyente Especial: NO RETENER (Art.92 LORTI)

Entradas:
  - megre_proveedores_clasificacion_ciiu.psv (o .xlsx)
    Columnas requeridas: tipo_concepto_ir, tipo_contribuyente, clase_contribuyente,
    categoria, obligado_llevar_contabilidad, agente_retencion, contribuyente_especial

Salida:
  - estimacion_renta_masiva.xlsx
    Columnas: numero_ruc, razon_social, tipo_concepto_ir, condicion_proveedor,
              codigo_sri, retencion_renta_pct, articulo, razon

Normativa: NAC-DGERCGC26-00000009 (vigente 01-03-2026)
           Art.97.10 LORTI (RIMPE)
================================================================================
"""

import duckdb
import pandas as pd
from pathlib import Path

BASE_DIR  = Path(__file__).parent.parent.parent   # → RETENCIONES/
XLSX_RUCS = BASE_DIR / "megre_proveedores_clasificacion_ciiu.xlsx"
OUT_EXCEL = Path(__file__).parent / "estimacion_renta_masiva.xlsx"


# ==============================================================================
# 1. REGLAS DE RETENCIÓN RENTA
#
#    Tasas: NAC-DGERCGC26-00000009 (2026)
#    Códigos SRI: de reglas_retencion_renta.py
#
#    Prioridad: menor número = mayor prioridad
#      0    → Contribuyente Especial (NO RETENER)
#      1-2  → RIMPE (siempre gana, independiente del tipo de actividad)
#      5    → overrides por condicion_proveedor específica (SOCIEDAD)
#      10   → reglas generales 10% (intelecto, profesional PN, arrendamiento)
#      20   → reglas generales 5% (profesional SOCIEDAD — cubierto en prio 5)
#      25   → reglas generales 3% (mano de obra, medios, financiero)
#      30   → reglas generales 2% (bienes muebles, construcción, energía, seguros)
#      35   → reglas generales 1% (transporte, agropecuario productor)
#      40   → agropecuario comercializador 1.75%
#      50   → residual 3%
# ==============================================================================

REGLAS_RENTA = [
    # (id, condicion_proveedor, tipo_concepto_ir, codigo_sri, porcentaje, prioridad, articulo, descripcion)

    # ── CONTRIBUYENTE ESPECIAL ──────────────────────────────────────────────
    ( 0, "CONTRIB_ESPECIAL", None, "N/A",  0.00,  0, "Art.92 LORTI", "Contribuyente Especial: NO RETENER"),

    # ── RIMPE ────────────────────────────────────────────────────────────────
    ( 1, "RIMPE_NEGOCIO_POP", None, "332",  0.00,  1, "Art.1.1.c / Art.97.10 LORTI",   "RIMPE Negocio Popular: exento"),
    ( 2, "RIMPE_EMPRENDEDOR", None, "343",  1.00,  2, "Art.1.2.c NAC-DGERCGC26-00000009", "RIMPE Emprendedor: 1%"),

    # ── Overrides SOCIEDAD (prio 5, deben ganar a reglas NULL PN) ────────────
    ( 3, "SOCIEDAD", "SERVICIO_PROFESIONAL",    "303A", 5.00, 5, "Art.1.6.a", "Servicios profesionales sociedad: 5%"),
    ( 4, "SOCIEDAD", "SERVICIO_INTELECTO",      "303A", 5.00, 5, "Art.1.6.a", "Servicios intelecto sociedad: 5%"),
    ( 5, "SOCIEDAD", "EDUCACION",               "303A", 5.00, 5, "Art.1.6.a", "Educación/docencia sociedad: 5%"),
    ( 6, "SOCIEDAD", "ARRENDAMIENTO_MERCANTIL", "319",  2.00, 5, "Art.1.4.g", "Arrendamiento mercantil sociedad: 2%"),
    ( 7, "SOCIEDAD", "COMISIONES",              "3482", 5.00, 5, "Art.1.6.a", "Comisiones sociedad: 5%"),

    # ── 15%: loterías, rifas, apuestas ──────────────────────────────────────
    ( 8, None, "LOTERIAS",                "335", 15.00,  8, "Art.1.8",   "Loterías, rifas, apuestas: 15%"),

    # ── 10%: intelecto, profesional PN, imagen, arrendamiento inmueble, comisiones PN
    ( 9, None, "SERVICIO_PROFESIONAL",    "303",  10.00, 10, "Art.1.7.a", "Honorarios profesionales PN: 10%"),
    (10, None, "SERVICIO_INTELECTO",      "304",  10.00, 10, "Art.1.7.a", "Servicios intelecto PN: 10%"),
    (11, None, "EDUCACION",               "304E", 10.00, 10, "Art.1.7.c", "Docencia: 10%"),
    (12, None, "IMAGEN_RENOMBRE",         "308",  10.00, 10, "Art.1.7.b", "Imagen, renombre, influencers: 10%"),
    (13, None, "ARRENDAMIENTO_INMUEBLE",  "320",  10.00, 10, "Art.1.7.g", "Arrendamiento inmueble: 10%"),
    (26, None, "COMISIONES",              "304A", 10.00, 10, "Art.1.7.a", "Comisiones PN: 10%"),

    # ── 3%: mano de obra, medios, financiero, liquidación, doméstico ────────
    (14, None, "SERVICIO_MANO_OBRA",   "307",  3.00, 25, "Art.1.5.a", "Mano de obra PN: 3%"),
    (15, None, "MEDIOS_COMUNICACION",  "309",  3.00, 25, "Art.1.5.c", "Publicidad y medios: 3%"),
    (16, None, "FINANCIERO_OTROS",     "323",  3.00, 25, "Art.1.5.d", "Rendimientos financieros: 3%"),
    (17, None, "DOMESTICO",            "307",  3.00, 25, "Art.1.5.a", "Doméstico - mano de obra: 3%"),

    # ── 2%: bienes muebles, construcción, energía, seguros, arrend. mercantil
    (18, None, "BIEN_MUEBLE",             "312",  2.00, 30, "Art.1.4.i", "Bienes muebles: 2%"),
    (19, None, "CONSTRUCCION",            "343B", 2.00, 30, "Art.1.4.h", "Construcción: 2%"),
    (20, None, "ENERGIA",                 "343A", 2.00, 30, "Art.1.4.a", "Energía eléctrica: 2%"),
    (21, None, "SEGUROS",                 "322",  2.00, 30, "Art.1.4.c", "Seguros y reaseguros: 2%"),
    (22, None, "ARRENDAMIENTO_MERCANTIL", "3440", 3.00, 30, "Art.3",     "Arrendamiento mercantil PN: residual 3%"),

    # ── 1%: transporte, agropecuario productor ───────────────────────────────
    (23, None, "TRANSPORTE",           "310",  1.00, 35, "Art.1.2.a", "Transporte: 1%"),

    # ── 1.75%: agropecuario comercializador (conservador por defecto) ────────
    (24, None, "BIEN_AGROPECUARIO",    "312C", 1.75, 40, "Art.1.3.a", "Agropecuario comercializador (conservador): 1.75%"),

    # ── Residual ──────────────────────────────────────────────────────────────
    (25, None, None,                   "3440", 3.00, 50, "Art.3",     "Residual: 3%"),
]

COLS_REGLAS = ["id", "condicion_proveedor", "tipo_concepto_ir",
               "codigo_sri", "porcentaje", "prioridad", "articulo", "descripcion"]


# ==============================================================================
# 2. DERIVAR condicion_proveedor
# ==============================================================================

SQL_CONDICION_PROVEEDOR = """
    CASE
        WHEN contribuyente_especial = 1                                                   THEN 'CONTRIB_ESPECIAL'
        WHEN clase_contribuyente = 'RIMPE' AND categoria = 'NEGOCIO POPULAR'              THEN 'RIMPE_NEGOCIO_POP'
        WHEN clase_contribuyente = 'RIMPE' AND categoria = 'EMPRENDEDOR'                  THEN 'RIMPE_EMPRENDEDOR'
        WHEN tipo_contribuyente  = 'SOCIEDAD'                                             THEN 'SOCIEDAD'
        WHEN tipo_contribuyente  = 'PERSONA NATURAL' AND obligado_llevar_contabilidad = 1 THEN 'PN_OBLIGADA'
        WHEN tipo_contribuyente  = 'PERSONA NATURAL' AND obligado_llevar_contabilidad = 0 THEN 'PN_NO_OBLIGADA'
        ELSE 'NO_CALIFICADO'
    END
"""


# ==============================================================================
# 3. MOTOR DUCKDB
# ==============================================================================

def build_motor(conn: duckdb.DuckDBPyConnection):
    df_reglas = pd.DataFrame(REGLAS_RENTA, columns=COLS_REGLAS)
    conn.register("reglas_renta_simple", df_reglas)


def calcular_renta_masiva(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute(f"""
        WITH rucs AS (
            SELECT
                numero_ruc,
                razon_social,
                estado_contribuyente,
                tipo_contribuyente,
                clase_contribuyente,
                categoria,
                tipo_concepto_ir,
                {SQL_CONDICION_PROVEEDOR} AS condicion_proveedor
            FROM rucs_raw
            WHERE estado_contribuyente = 'ACTIVO'
              AND tipo_concepto_ir IS NOT NULL
        ),
        candidatas AS (
            SELECT
                r.numero_ruc,
                r.razon_social,
                r.tipo_concepto_ir,
                r.condicion_proveedor,
                rg.codigo_sri,
                rg.porcentaje,
                rg.articulo,
                rg.descripcion,
                rg.prioridad,
                ROW_NUMBER() OVER (
                    PARTITION BY r.numero_ruc
                    ORDER BY rg.prioridad ASC
                ) AS rn
            FROM rucs r
            JOIN reglas_renta_simple rg
              ON (rg.condicion_proveedor IS NULL OR rg.condicion_proveedor = r.condicion_proveedor)
             AND (rg.tipo_concepto_ir    IS NULL OR rg.tipo_concepto_ir    = r.tipo_concepto_ir)
        )
        SELECT
            numero_ruc,
            razon_social,
            tipo_concepto_ir,
            condicion_proveedor,
            codigo_sri,
            porcentaje                    AS retencion_renta_pct,
            articulo,
            CONCAT(
                condicion_proveedor, ' + ', COALESCE(tipo_concepto_ir, 'cualquier tipo'),
                ' → ', codigo_sri, ' (', CAST(porcentaje AS VARCHAR), '%) ',
                '| ', articulo
            ) AS razon
        FROM candidatas
        WHERE rn = 1
        ORDER BY retencion_renta_pct DESC, condicion_proveedor, tipo_concepto_ir
    """).df()


# ==============================================================================
# 4. MAIN
# ==============================================================================

def main():
    print("Cargando datos...")
    df_rucs = pd.read_excel(XLSX_RUCS)
    print(f"  {len(df_rucs):,} RUCs cargados")

    conn = duckdb.connect()
    conn.register("rucs_raw", df_rucs)
    build_motor(conn)

    print("Calculando retenciones Renta...")
    resultado = calcular_renta_masiva(conn)
    print(f"  {len(resultado):,} RUCs procesados")

    print("\n── Distribución por código SRI ─────────────────────────")
    resumen = (
        resultado.groupby(["codigo_sri", "retencion_renta_pct"])
        .size()
        .reset_index(name="rucs")
        .sort_values("retencion_renta_pct")
    )
    print(resumen.to_string(index=False))

    print("\n── Distribución por condición proveedor ────────────────")
    print(
        resultado.groupby("condicion_proveedor")["retencion_renta_pct"]
        .agg(["count", "mean", "min", "max"])
        .rename(columns={"count": "rucs", "mean": "pct_promedio", "min": "pct_min", "max": "pct_max"})
        .round(2)
        .to_string()
    )

    resultado.to_excel(OUT_EXCEL, index=False)
    print(f"\nGuardado: {OUT_EXCEL}")


if __name__ == "__main__":
    main()
