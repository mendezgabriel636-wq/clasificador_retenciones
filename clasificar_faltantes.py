"""
Clasifica las actividades económicas faltantes usando reglas de texto.
Sin API — basado en palabras clave del nombre de la actividad.

Ejecución: uv run python clasificar_faltantes.py
"""
import re
import duckdb

DUCKDB_PATH = "catalogo_retenciones.duckdb"

IVA_BIENES    = "bienes gravados con iva"
IVA_SERVICIOS = "servicios y derechos, comisiones por intermediacion, contratos de consultoria"
IVA_PROF      = "servicios profesionales personas naturales con titulo universitario"
IVA_CONST     = "servicios de construccion"
IVA_ARRIENDO  = "arrendamiento de inmuebles de personas naturales o sucesiones indivisas no obligadas a llevar contabilidad"


def contiene(texto: str, *palabras) -> bool:
    return any(p in texto for p in palabras)


def clasificar(actividad: str) -> tuple[str, str]:
    t = actividad.lower()

    # ── CONSTRUCCIÓN ─────────────────────────────────────────────────────────
    if contiene(t, "construcción", "construccion", "obra civil", "edificación",
                "plomería", "fontanería", "electricidad e inst", "instalación eléctr",
                "pavimentación", "excavación", "demolición", "carpintería en obras"):
        return IVA_CONST, "CONSTRUCCION"

    # ── TRANSPORTE ───────────────────────────────────────────────────────────
    if contiene(t, "transporte de", "transportación", "servicio de taxi",
                "servicio de bus", "flete", "mudanza", "courier", "mensajería"):
        return IVA_SERVICIOS, "TRANSPORTE"

    # ── EXTRACCIÓN MINERA ────────────────────────────────────────────────────
    if contiene(t, "extracción de petróleo", "extracción de gas", "extracción de mineral",
                "extracción de carbón", "extracción de sal", "extracción de piedra",
                "extracción de arena", "extracción de grava", "explotación de minas",
                "explotación de canteras", "minería", "extracción de cobre",
                "extracción de oro", "extracción de plata", "extracción de zinc"):
        return IVA_BIENES, "MINERALES"

    # ── ENERGÍA ──────────────────────────────────────────────────────────────
    if contiene(t, "generación de energía", "distribución de energía", "transmisión de energía",
                "generación de electricidad", "distribución de electricidad",
                "suministro de agua", "suministro de gas"):
        return IVA_SERVICIOS, "ENERGIA"

    # ── AGRICULTURA / GANADERÍA / PESCA ──────────────────────────────────────
    if contiene(t, "cultivo de", "cosecha de", "siembra de", "plantación de",
                "crianza de", "cría de", "ganadería", "avicultura", "apicultura",
                "piscicultura", "acuicultura", "pesca ", "caza ", "buques-factoría",
                "actividades de caza", "albergue y cuidad", "animales de granja",
                "cultivo ostras", "cultivo de laver", "cultivo de algas",
                "recolección de", "silvicultura", "extracción de madera",
                "extracción de leña", "acondicionamiento y mantenimiento de terrenos para usos agrícolas"):
        return IVA_BIENES, "BIEN_AGROPECUARIO"

    # ── FABRICACIÓN / ELABORACIÓN / PRODUCCIÓN DE BIENES ────────────────────
    bienes_keywords = [
        "fabricación de", "elaboración de", "producción de",
        "molienda de", "hilatura de", "tejido de", "confección de",
        "manufactura de", "ensamblaje de", "fundición de", "laminación de",
        "estampado de", "forjado de", "trefilado de", "corte de",
        "curtido de", "aserrado de", "trituración de", "refinación de",
        "destilación de", "fermentación de", "mezcla de",
        "procesamiento de", "conservación de pescado", "conservación de frutas",
        "conservación de carne", "descafeinado de",
        "torrefacción de", "tostado de", "deshidratación de",
        "enlatado de", "embotellado de", "preparación de harina",
        "preparación de almidón", "preparación de aceite",
        "preparación de azúcar", "preparación de café",
        "preparación de cacao",
    ]
    if contiene(t, *bienes_keywords):
        # Sub-clasificación IR dentro de bienes
        if contiene(t, "alimento", "comida", "bebida", "carne", "pescado",
                    "marisco", "lácteo", "cereal", "harina", "aceite", "azúcar",
                    "café", "cacao", "fruta", "verdura", "vegetal", "grasa"):
            return IVA_BIENES, "BIEN_AGROPECUARIO"
        return IVA_BIENES, "BIEN_MUEBLE"

    # ── VENTA AL POR MAYOR / MENOR ───────────────────────────────────────────
    if contiene(t, "venta al por mayor", "venta al por menor",
                "comercio al por mayor", "comercio al por menor"):
        if contiene(t, "alimento", "comida", "bebida", "carne", "pescado",
                    "fruta", "verdura", "cereal", "harina", "aceite", "lácteo"):
            return IVA_BIENES, "BIEN_AGROPECUARIO"
        return IVA_BIENES, "BIEN_MUEBLE"

    # ── ACTIVIDADES ADMINISTRATIVAS (fabricación subcontratada) ──────────────
    if contiene(t, "actividades administrativas de", "actividades administrativas vinculadas"):
        if contiene(t, "prenda", "confección", "vestir", "textil"):
            return IVA_BIENES, "BIEN_MUEBLE"
        return IVA_SERVICIOS, "RESIDUAL"

    # ── ALQUILER / ARRENDAMIENTO ─────────────────────────────────────────────
    if contiene(t, "arrendamiento de inmueble", "alquiler de inmueble",
                "arrendamiento de vivienda", "alquiler de vivienda",
                "alquiler de apartamento", "arrendamiento de local",
                "alquiler de local", "arrendamiento de terreno",
                "alquiler de terreno", "arrendamiento de finca",
                "alquiler y venta de tumbas"):
        return IVA_ARRIENDO, "ARRENDAMIENTO_INMUEBLE"

    if contiene(t, "arrendamiento de maquinaria", "alquiler de maquinaria",
                "arrendamiento de equipo", "alquiler de equipo",
                "arrendamiento de vehículo", "alquiler de vehículo",
                "arrendamiento de automóvil", "alquiler de automóvil",
                "arrendamiento mercantil", "leasing"):
        return IVA_SERVICIOS, "ARRENDAMIENTO_MERCANTIL"

    # ── SERVICIOS MÉDICOS / SALUD ─────────────────────────────────────────────
    if contiene(t, "atención médica", "atención de la salud", "actividades médica",
                "hospital", "clínica", "consultorio", "diagnóstico médico",
                "cirugía", "odontología", "psicología", "fisioterapia",
                "acupuntura", "quiropráctica", "laboratorio clínico",
                "veterinaria", "servicios veterinario",
                "casas de reposo", "hogares de transición",
                "asistencia social", "ayuda a refugiados"):
        return IVA_PROF, "SERVICIO_PROFESIONAL"

    # ── EDUCACIÓN ────────────────────────────────────────────────────────────
    if contiene(t, "enseñanza", "educación", "capacitación", "formación",
                "instrucción", "academia", "escuela ", "colegio ", "universidad",
                "instituto de", "cursos de"):
        return IVA_SERVICIOS, "EDUCACION"

    # ── SERVICIOS FINANCIEROS ─────────────────────────────────────────────────
    if contiene(t, "banco", "cooperativa de ahorro", "captación de depósitos",
                "administración de mercados financieros",
                "actividades de bolsa", "casas de cambio",
                "financiamiento"):
        return IVA_SERVICIOS, "FINANCIERO_BANCO"

    if contiene(t, "seguros de", "reaseguros", "actividades de seguros"):
        return IVA_SERVICIOS, "SEGUROS"

    if contiene(t, "fondos de pensión", "fondo de inversión",
                "administración de cartera", "actividades fiduciaria"):
        return IVA_SERVICIOS, "FINANCIERO_OTROS"

    # ── ALMACENAMIENTO / DEPÓSITO ─────────────────────────────────────────────
    if contiene(t, "almacenamiento", "depósito de", "silos de"):
        return IVA_SERVICIOS, "SERVICIO_MANO_OBRA"

    # ── MEDIOS / PUBLICIDAD ───────────────────────────────────────────────────
    if contiene(t, "publicidad", "televisión", "radio ", "prensa", "periódico",
                "revista", "transmisión de", "difusión de", "medios de comunicación"):
        return IVA_SERVICIOS, "MEDIOS_COMUNICACION"

    # ── PUBLICACIÓN (editorial, software) ────────────────────────────────────
    if contiene(t, "publicación de", "edición de"):
        return IVA_SERVICIOS, "SERVICIO_INTELECTO"

    # ── COMISIONES / AGENCIA ──────────────────────────────────────────────────
    if contiene(t, "intermediario", "intermediarios del comercio",
                "comisioni", "agencia de", "agentes de"):
        return IVA_SERVICIOS, "COMISIONES"

    # ── SERVICIOS PÚBLICOS / ASOCIACIONES ────────────────────────────────────
    if contiene(t, "entidad del estado", "empresa pública", "municipio",
                "gobierno", "administración pública"):
        return IVA_SERVICIOS, "SECTOR_PUBLICO"

    if contiene(t, "asociación de", "asociaciones de", "sindicato",
                "federación de", "organización sin fines de lucro",
                "clubes y organizaciones", "logias", "congregación",
                "orden religiosa", "iglesia ", "asociaciones con fines"):
        return IVA_SERVICIOS, "RESIDUAL"

    # ── ACTIVIDADES DE* (genéricas) ───────────────────────────────────────────
    if t.startswith("actividades de"):
        if contiene(t, "pesca", "caza", "buques"):
            return IVA_BIENES, "BIEN_AGROPECUARIO"
        if contiene(t, "extracción"):
            return IVA_BIENES, "MINERALES"
        if contiene(t, "construcción", "construccion"):
            return IVA_CONST, "CONSTRUCCION"
        if contiene(t, "transporte", "flete"):
            return IVA_SERVICIOS, "TRANSPORTE"
        if contiene(t, "médic", "salud", "hospital", "clínica", "diagnóstic",
                    "acupuntura", "quiropráctic", "reposo", "discapacit",
                    "asistencia social", "refugiado", "inmigrante",
                    "beneficencia", "asistente de compras"):
            return IVA_PROF, "SERVICIO_PROFESIONAL"
        if contiene(t, "seguro", "reaseguro"):
            return IVA_SERVICIOS, "SEGUROS"
        if contiene(t, "banco", "financiero", "bolsa", "mercado financiero"):
            return IVA_SERVICIOS, "FINANCIERO_BANCO"
        if contiene(t, "alquiler", "arrendamiento"):
            return IVA_SERVICIOS, "ARRENDAMIENTO_MERCANTIL"
        if contiene(t, "almacenamiento", "depósito"):
            return IVA_SERVICIOS, "SERVICIO_MANO_OBRA"
        if contiene(t, "edición", "editorial", "publicación"):
            return IVA_SERVICIOS, "SERVICIO_INTELECTO"
        if contiene(t, "asociación", "club ", "fraternales", "jóvenes",
                    "veteranos", "patriótico", "masónica", "rotari",
                    "filantrop", "caritativ"):
            return IVA_SERVICIOS, "RESIDUAL"
        # Genérico → servicios
        return IVA_SERVICIOS, "RESIDUAL"

    # ── FALLBACK ──────────────────────────────────────────────────────────────
    return IVA_SERVICIOS, "RESIDUAL"


def main() -> None:
    con = duckdb.connect(DUCKDB_PATH)

    faltantes = con.execute("""
        SELECT DISTINCT b.actividad_economica
        FROM base_rucs_sri b
        LEFT JOIN actividades_economicas_clasificadas c
            ON b.actividad_economica = c.actividad_economica
        WHERE b.matriz = 1
          AND b.actividad_economica IS NOT NULL
          AND b.actividad_economica != ''
          AND c.actividad_economica IS NULL
        ORDER BY 1
    """).fetchall()

    total = len(faltantes)
    print(f"Clasificando {total} actividades por reglas de texto...")

    conteo_ir: dict[str, int] = {}
    insertadas = 0

    for (actividad,) in faltantes:
        iva, ir = clasificar(actividad)
        conteo_ir[ir] = conteo_ir.get(ir, 0) + 1
        con.execute(
            "INSERT INTO actividades_economicas_clasificadas "
            "(actividad_economica, codigo_ciiu, tipo_concepto_iva, tipo_concepto_ir) "
            "VALUES (?, NULL, ?, ?)",
            [actividad, iva, ir],
        )
        insertadas += 1

    print(f"Insertadas: {insertadas}/{total}\n")
    print("Distribución por tipo_concepto_ir:")
    for ir, n in sorted(conteo_ir.items(), key=lambda x: -x[1]):
        print(f"  {n:4d}  {ir}")

    # Verificación final
    sin = con.execute("""
        SELECT COUNT(DISTINCT b.actividad_economica)
        FROM base_rucs_sri b
        LEFT JOIN actividades_economicas_clasificadas c ON b.actividad_economica = c.actividad_economica
        WHERE b.matriz=1 AND b.actividad_economica IS NOT NULL AND b.actividad_economica != ''
        AND c.actividad_economica IS NULL
    """).fetchone()[0]
    total_tabla = con.execute("SELECT COUNT(*) FROM actividades_economicas_clasificadas").fetchone()[0]
    print(f"\nTotal en catálogo: {total_tabla:,}  |  Sin clasificar: {sin}")
    con.close()


if __name__ == "__main__":
    main()
