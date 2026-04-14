import polars as pl
from sqlalchemy.engine import Engine


def consulta_sql(engine_data_fact: Engine) -> tuple[pl.DataFrame, pl.DataFrame]:
    query_sri = """
    SELECT
        numero_ruc,
        razon_social,
        estado_contribuyente,
        estado_establecimiento,
        actividad_economica,
        tipo_contribuyente,
        clase_contribuyente,
        categoria,
        obligado_llevar_contabilidad,
        agente_retencion,
        contribuyente_especial,
        fecha_actualizacion
    FROM base_rucs_sri;
    """

    # =========================================================================
    # CAMBIO: Se amplió el SELECT para traer columnas necesarias para el
    #         formato final de carga al RDS (antes solo traía numero_ruc,
    #         codigo_ciiu, actividad_economica)
    # =========================================================================
    query_catastro = """
    SELECT
        numero_ruc,
        codigo_ciiu,
        actividad_economica,
        nombre_fantasia_comercial,
        estado_establecimiento,
        numero_establecimiento,
        id_establecimiento,
        fecha_inicio_actividades_comercio,
        fecha_actualizacion_comercio,
        fecha_cese_comercio,
        fecha_reinicio_actividades_comercio,
        direccion_completa,
        motivo_cancelacion_suspension,
        contribuyente_fantasma,
        transacciones_inexistente,
        nombre_representante_legal,
        identificacion_representante_legal,
        representantes_legales
    FROM base_rucs_catastro;
    """

    df_base_rucs_sri = pl.read_database(query_sri, connection=engine_data_fact)
    df_base_rucs_catastro = pl.read_database(query_catastro, connection=engine_data_fact)

    return df_base_rucs_sri, df_base_rucs_catastro


def consulta_excel() -> pl.DataFrame:
    return pl.read_excel(r"..\bases_datos\ciiu_nivel6.xlsx")


def consulta_excel_correcciones() -> pl.DataFrame:
    return (
        pl.read_excel(r"..\bases_datos\correccion_final.xlsx")
        .select(["actividad_economica", "codigo_ciiu", "descripcion_ciiu"])
        .with_columns(pl.col("descripcion_ciiu").str.to_uppercase())
    )


# =========================================================================
# CAMBIO: Lista de columnas extra de catastro que deben propagarse en todos
#         los selects intermedios para que lleguen al DataFrame final
# =========================================================================
_COLS_EXTRA_CATASTRO = [
    "nombre_fantasia_comercial",
    "estado_establecimiento",
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


def procesamiento(engine_data_fact: Engine) -> pl.DataFrame:
    df_base_rucs_sri, df_base_rucs_catastro = consulta_sql(engine_data_fact)
    df_inec = consulta_excel()

    # =====================
    # Procesamiento Catastro
    # =====================
    df_base_rucs_catastro = (
        df_base_rucs_catastro
        .rename({"actividad_economica": "actividad_economica_catastro"})
        .unique(subset=["numero_ruc"])
    )

    # =====================
    # Procesamiento INEC
    # =====================
    df_inec = df_inec.drop(["NIVEL", "__UNNAMED__0"])

    df_inec = (
        df_inec
        .with_columns(pl.col("CODIGO").str.replace(r"\.", ""))
        .with_columns(pl.col("DESCRIPCION").str.strip_chars().str.to_uppercase())
    )

    df_inec = df_inec.rename({"DESCRIPCION": "DESCRIPCION_INEC"})

    # Comprobamos si hay más ciiu en catastro que en inec
    ciiu_catastro = set(df_base_rucs_catastro["codigo_ciiu"].to_list())
    ciiu_inec = set(df_inec["CODIGO"].to_list())
    longitud_ciiu_catastros_no_inec = len(ciiu_catastro - ciiu_inec)

    if longitud_ciiu_catastros_no_inec > 0:
        print(f"Hay {longitud_ciiu_catastros_no_inec} más códigos en catastro que en inec")
    else:
        print("Hay igual o más en Inec que en catastro")

    # =====================
    # Procesamiento Base_rucs_sri
    # =====================

    # Nos quedamos con rucs válidos
    df_base_rucs_sri = df_base_rucs_sri.filter(
        pl.col("numero_ruc").cast(pl.Utf8).str.len_chars() >= 9
    )

    # Quitamos duplicados por fecha
    df_base_rucs_sri = (
        df_base_rucs_sri
        .sort("fecha_actualizacion", descending=True)
        .unique(subset=["numero_ruc"])
    )

    # Nos quedamos con las columnas importantes
    df_base_rucs_sri = df_base_rucs_sri.select([
        "numero_ruc",
        "actividad_economica",
        "razon_social",
        "estado_contribuyente",
        "clase_contribuyente",
        "categoria",
        "obligado_llevar_contabilidad",
        "agente_retencion",
        "contribuyente_especial"
    ])

    # =====================
    # Joins: sri <--- catastro <--- inec
    # =====================
    catastro_inec = df_base_rucs_catastro.join(
        df_inec, right_on="CODIGO", left_on="codigo_ciiu", how="left"
    )
    sri_catastro_inec = df_base_rucs_sri.join(
        catastro_inec, on="numero_ruc", how="left"
    )

    # Filtramos los que tienen actividad económica en sri pero no tienen INEC
    catastro_sri_no_inec = sri_catastro_inec.filter(
        pl.col("DESCRIPCION_INEC").is_null() & pl.col("actividad_economica").is_not_null()
    )

    longitud_catastro_sri_no_inec = len(
        catastro_sri_no_inec.unique(subset=["actividad_economica"])
    )
    print(f"Hay {longitud_catastro_sri_no_inec} actividades económicas que tienen catastro y sri pero no inec")

    # =========================================================================
    # CAMBIO: Se agregaron _COLS_EXTRA_CATASTRO al select para que las columnas
    #         de catastro se propaguen a los DataFrames derivados
    # =========================================================================
    catastro_sri_no_inec = catastro_sri_no_inec.select([
        "numero_ruc", "actividad_economica", "razon_social",
        "estado_contribuyente", "clase_contribuyente", "categoria",
        "obligado_llevar_contabilidad", "agente_retencion", "contribuyente_especial",
    ] + _COLS_EXTRA_CATASTRO)

    catastro_sri_no_inec_aumentado_inec = catastro_sri_no_inec.join(
        df_inec, left_on="actividad_economica", right_on="DESCRIPCION_INEC", how="left"
    )

    # =====================
    # Corrección semántica desde Excel
    # =====================
    faltantes_correccion_semantica = (
        catastro_sri_no_inec_aumentado_inec
        .filter(pl.col("actividad_economica").is_not_null() & pl.col("CODIGO").is_null())
        .with_columns(pl.col("actividad_economica").str.strip_chars())
    )

    correcciones = consulta_excel_correcciones()

    corregido_semanticamente = (
        faltantes_correccion_semantica
        .join(correcciones, on="actividad_economica", how="left")
        .drop("CODIGO")
        .rename({"codigo_ciiu": "CODIGO", "descripcion_ciiu": "DESCRIPCION_INEC"})
    )

    # =====================
    # Juntar todo
    # =====================

    # Los que fueron corregidos al hacer join entre catastro e inec
    corregidos_catastro_inec = sri_catastro_inec.filter(
        pl.col("DESCRIPCION_INEC").is_not_null()
    )

    # Los que fueron corregidos a partir de los que tienen sri y no inec
    corregidos_sri_no_inec = catastro_sri_no_inec_aumentado_inec.filter(
        pl.col("actividad_economica").is_not_null() & pl.col("CODIGO").is_not_null()
    )

    corregidos_catastro_inec = (
        corregidos_catastro_inec
        .drop("actividad_economica_catastro")
        .rename({"codigo_ciiu": "CODIGO"})
        .with_columns(pl.lit(1).alias("correcion_catastro_inec_ciiu"))
    )

    corregidos_sri_no_inec = (
        corregidos_sri_no_inec
        .with_columns([
            pl.col("razon_social").alias("DESCRIPCION_INEC"),
            pl.lit(1).alias("correcion_sri_inec_desc"),
        ])
    )

    corregido_semanticamente = (
        corregido_semanticamente
        .with_columns(pl.lit(1).alias("correcion_semantica"))
    )

    # Concatenar y guardar
    base_rucs_sri_corregido_catastro = pl.concat(
        [corregidos_catastro_inec, corregidos_sri_no_inec, corregido_semanticamente],
        how="diagonal",
    )



    print(len(base_rucs_sri_corregido_catastro.filter(pl.col("CODIGO").is_null()).unique("actividad_economica")['actividad_economica']))

    print(f"Archivo guardado con {len(base_rucs_sri_corregido_catastro.unique('numero_ruc'))} registros únicos")

    rucs_sri_corregida = base_rucs_sri_corregido_catastro.unique("numero_ruc")

    #Leemos base_que_hace_merge_ciiu_bien_servicio
    ciiu_clasificado = pl.read_excel(r"..\bases_datos\ciiu_clasificado_retencion_iva_bien_servicio_v6.xlsx")

    #Procesamos el ciiu_clasificado
    ciiu_clasificado = ciiu_clasificado.with_columns(pl.col("codigo_ciiu").str.replace_all(".", "", literal=True).alias("codigo_ciiu_sin_punto"))

    ciiu_clasificado = ciiu_clasificado.drop(["codigo_ciiu", "descripcion", "nivel", "confianza", "Claude_clasificacion_probabilidad"])
    
    #Adjuntamos la clasificación
    merge_sri_clasificacion_ciiu = rucs_sri_corregida.join(ciiu_clasificado,
                    right_on = "codigo_ciiu_sin_punto",
                    left_on ='CODIGO',
                    how = "left")
    
    #Adjuntamos la clasificación
    merge_sri_clasificacion_ciiu.write_parquet(r"..\bases_datos\rucs_sri_corregida_ciiu_clasificado.parquet")

    return merge_sri_clasificacion_ciiu
