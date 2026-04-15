"""
Script de test local — corre el pipeline completo sin conectarse al RDS.
Lee base_rucs_sri y catastro desde parquet en ../bases_datos/
"""

import polars as pl
import time

from retencion_iva import aplicar_retencion_iva
from retencion_renta import aplicar_retencion_renta


# =========================================================================
# Columnas extra de SRI que deben propagarse en los selects intermedios.
# NO incluye columnas que ya están en el select base:
#   estado_establecimiento, tipo_contribuyente
# =========================================================================
_COLS_EXTRA_SRI = [
    "nombre_fantasia_comercial",
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


def consulta_sql_local() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Reemplaza consulta_sql() — lee parquets en vez de SQL"""

    df_base_rucs_sri = pl.read_parquet(r"..\bases_datos\base_rucs_sri.parquet")

    df_base_rucs_sri = df_base_rucs_sri.select([
        "numero_ruc", "razon_social", "estado_contribuyente",
        "estado_establecimiento", "actividad_economica",
        "tipo_contribuyente",
        "clase_contribuyente", "categoria", "obligado_llevar_contabilidad",
        "agente_retencion", "contribuyente_especial", "fecha_actualizacion",
    ] + _COLS_EXTRA_SRI)

    df_base_rucs_catastro = pl.read_parquet(r"..\bases_datos\catastro.parquet")

    return df_base_rucs_sri, df_base_rucs_catastro


def procesamiento_local() -> pl.DataFrame:
    """Replica procesamiento() pero sin engine — lee de parquet"""

    df_base_rucs_sri, df_base_rucs_catastro = consulta_sql_local()
    df_inec = pl.read_excel(r"..\bases_datos\ciiu_nivel6.xlsx")

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
    df_base_rucs_sri = df_base_rucs_sri.filter(
        pl.col("numero_ruc").cast(pl.Utf8).str.len_chars() >= 9
    )
    df_base_rucs_sri = (
        df_base_rucs_sri
        .sort("fecha_actualizacion", descending=True)
        .unique(subset=["numero_ruc"])
    )

    # CAMBIO: tipo_contribuyente incluido (lo necesitan retencion_iva y retencion_renta)
    df_base_rucs_sri = df_base_rucs_sri.select([
        "numero_ruc", "actividad_economica", "razon_social",
        "estado_contribuyente", "estado_establecimiento",
        "tipo_contribuyente",
        "clase_contribuyente", "categoria",
        "obligado_llevar_contabilidad", "agente_retencion", "contribuyente_especial",
    ] + _COLS_EXTRA_SRI)

    # =====================
    # Joins
    # =====================
    catastro_inec = df_base_rucs_catastro.join(
        df_inec, right_on="CODIGO", left_on="codigo_ciiu", how="left"
    )
    sri_catastro_inec = df_base_rucs_sri.join(
        catastro_inec, on="numero_ruc", how="left"
    )

    catastro_sri_no_inec = sri_catastro_inec.filter(
        pl.col("DESCRIPCION_INEC").is_null() & pl.col("actividad_economica").is_not_null()
    )

    print(f"Hay {len(catastro_sri_no_inec.unique(subset=['actividad_economica']))} actividades económicas que tienen catastro y sri pero no inec")

    # CAMBIO: tipo_contribuyente incluido
    catastro_sri_no_inec = catastro_sri_no_inec.select([
        "numero_ruc", "actividad_economica", "razon_social",
        "estado_contribuyente", "estado_establecimiento",
        "tipo_contribuyente",
        "clase_contribuyente", "categoria",
        "obligado_llevar_contabilidad", "agente_retencion", "contribuyente_especial",
    ] + _COLS_EXTRA_SRI)

    catastro_sri_no_inec_aumentado_inec = catastro_sri_no_inec.join(
        df_inec, left_on="actividad_economica", right_on="DESCRIPCION_INEC", how="left"
    )

    # Corrección semántica
    faltantes_correccion_semantica = (
        catastro_sri_no_inec_aumentado_inec
        .filter(pl.col("actividad_economica").is_not_null() & pl.col("CODIGO").is_null())
        .with_columns(pl.col("actividad_economica").str.strip_chars())
    )

    correcciones = (
        pl.read_excel(r"..\bases_datos\correccion_final.xlsx")
        .select(["actividad_economica", "codigo_ciiu", "descripcion_ciiu"])
        .with_columns(pl.col("descripcion_ciiu").str.to_uppercase())
    )

    corregido_semanticamente = (
        faltantes_correccion_semantica
        .join(correcciones, on="actividad_economica", how="left")
        .drop("CODIGO")
        .rename({"codigo_ciiu": "CODIGO", "descripcion_ciiu": "DESCRIPCION_INEC"})
    )

    # Juntar todo
    corregidos_catastro_inec = (
        sri_catastro_inec.filter(pl.col("DESCRIPCION_INEC").is_not_null())
        .drop("actividad_economica_catastro")
        .rename({"codigo_ciiu": "CODIGO"})
        .with_columns(pl.lit(1).alias("correcion_catastro_inec_ciiu"))
    )

    corregidos_sri_no_inec = (
        catastro_sri_no_inec_aumentado_inec
        .filter(pl.col("actividad_economica").is_not_null() & pl.col("CODIGO").is_not_null())
        .with_columns([
            pl.col("razon_social").alias("DESCRIPCION_INEC"),
            pl.lit(1).alias("correcion_sri_inec_desc"),
        ])
    )

    corregido_semanticamente = corregido_semanticamente.with_columns(pl.lit(1).alias("correcion_semantica"))

    base_rucs_sri_corregido_catastro = pl.concat(
        [corregidos_catastro_inec, corregidos_sri_no_inec, corregido_semanticamente],
        how="diagonal",
    )

    print(len(base_rucs_sri_corregido_catastro.filter(pl.col("CODIGO").is_null()).unique("actividad_economica")['actividad_economica']))
    rucs_sri_corregida = base_rucs_sri_corregido_catastro.unique("numero_ruc")
    print(f"Registros únicos: {len(rucs_sri_corregida)}")

    # Clasificación CIIU
    ciiu_clasificado = pl.read_excel(r"..\bases_datos\ciiu_clasificado_retencion_iva_bien_servicio_v6.xlsx")
    ciiu_clasificado = ciiu_clasificado.with_columns(
        pl.col("codigo_ciiu").str.replace_all(".", "", literal=True).alias("codigo_ciiu_sin_punto")
    )
    ciiu_clasificado = ciiu_clasificado.drop(["codigo_ciiu", "descripcion", "nivel", "confianza", "Claude_clasificacion_probabilidad"])

    merge_sri_clasificacion_ciiu = rucs_sri_corregida.join(
        ciiu_clasificado, right_on="codigo_ciiu_sin_punto", left_on="CODIGO", how="left"
    )

    return merge_sri_clasificacion_ciiu


def formatear_para_rds(df: pl.DataFrame) -> pl.DataFrame:
    """Misma función de calculo_retenciones.py"""

    df = df.rename({
        'obligado_llevar_contabilidad': 'obligado',
        'CODIGO': 'codigo_ciiu',
        'porcentaje_renta': 'porcentaje_retencion_renta',
        'fecha_inicio_actividades_comercio': 'fecha_inicio_actividades',
        'fecha_actualizacion_comercio': 'fecha_actualizacion',
        'fecha_cese_comercio': 'fecha_suspension_definitiva',
        'fecha_reinicio_actividades_comercio': 'fecha_reinicio_actividades',
    })

    df = df.with_columns([
        pl.col('numero_ruc').cast(pl.Utf8)
          .str.replace(r'\.0$', '')
          .str.zfill(13)
          .alias('numero_ruc_str'),
        pl.col('nombre_fantasia_comercial').alias('nombre_comercial'),
        pl.col('direccion_completa').str.split_exact(' / ', 3)
          .struct.field('field_0').alias('descripcion_provincia_est'),
        pl.col('direccion_completa').str.split_exact(' / ', 3)
          .struct.field('field_1').alias('descripcion_canton_est'),
        pl.col('direccion_completa').str.split_exact(' / ', 3)
          .struct.field('field_2').alias('descripcion_parroquia_est'),
        pl.col('porcentaje_retencion_iva')
          .replace({10: 9, 20: 10, 30: 1, 50: 11, 70: 2, 100: 3}, default=0)
          .alias('codigo_anexo_iva'),
        pl.lit(None).cast(pl.Utf8).alias('provincia_jurisdiccion'),
        pl.lit(None).cast(pl.Utf8).alias('provincia_archivo_procesamiento'),
        pl.lit(None).cast(pl.Utf8).alias('nro_campo'),
        pl.lit(time.strftime('%Y-%m-%d')).alias('fecha_carga'),
    ])

    df = df.with_columns([
        pl.col('numero_ruc').cast(pl.Utf8).str.replace(r'001$', '').alias('cedula'),
        pl.col('numero_ruc_str').str.replace(r'001$', '').alias('cedula_str'),
    ])

    df = df.with_columns([
        pl.col('porcentaje_retencion_renta').cast(pl.Utf8),
        pl.col('porcentaje_retencion_iva').cast(pl.Utf8),
        pl.col('campo_formulario_104_iva').cast(pl.Utf8),
        pl.col('campo_formulario_103_ir').cast(pl.Utf8),
        pl.col('codigo_anexo_iva').cast(pl.Utf8),
    ])

    df = df.with_columns([
        pl.when(
            pl.col('obligado').cast(pl.Utf8).str.to_uppercase().is_in(['SI', 'SÍ', '1', 'TRUE', 'S'])
        ).then(1).otherwise(0).cast(pl.Int8).alias('obligado'),
        pl.col('agente_retencion').cast(pl.Utf8).str.to_uppercase()
          .map_elements(lambda v: 1 if v in ('SI', 'SÍ', '1', 'TRUE', 'S') else 0, return_dtype=pl.Int8)
          .alias('agente_retencion'),
        pl.col('contribuyente_especial').cast(pl.Utf8).str.to_uppercase()
          .map_elements(lambda v: 1 if v in ('SI', 'SÍ', '1', 'TRUE', 'S') else 0, return_dtype=pl.Int8)
          .alias('contribuyente_especial'),
        pl.col('contribuyente_fantasma').cast(pl.Utf8).str.to_uppercase()
          .map_elements(lambda v: 1 if v in ('SI', 'SÍ', '1', 'TRUE', 'S') else 0, return_dtype=pl.Int8)
          .alias('contribuyente_fantasma'),
        pl.col('transacciones_inexistente').cast(pl.Utf8).str.to_uppercase()
          .map_elements(lambda v: 1 if v in ('SI', 'SÍ', '1', 'TRUE', 'S') else 0, return_dtype=pl.Int8)
          .alias('transacciones_inexistente'),
    ])

    df = df.drop(['motivo_iva', 'base_calculo_renta', 'direccion_completa'])

    return df


def main():
    print("=" * 60)
    print("TEST LOCAL — Sin conexión a RDS")
    print("=" * 60)

    # 1. Procesamiento
    print("\n[1/4] Procesamiento base RUCs...")
    base = procesamiento_local()
    print(f"      Registros: {len(base)}")
    print(f"      Columnas:  {base.columns}")

    # 2. Retención IVA
    print("\n[2/4] Aplicando retención IVA...")
    aplicado_iva = aplicar_retencion_iva(base)
    print(f"      OK")

    # 3. Retención Renta
    print("\n[3/4] Aplicando retención Renta...")
    aplicado_renta = aplicar_retencion_renta(base)
    print(f"      OK")

    # 4. Join IVA + Renta
    print("\n[4/4] Uniendo IVA + Renta + Formateo RDS...")
    columnas_renta = ['numero_ruc', 'codigo_sri_renta', 'porcentaje_renta', 'descripcion_renta', 'base_calculo_renta']

    iva_renta = aplicado_iva.join(
        aplicado_renta.select(columnas_renta),
        on='numero_ruc',
        how='left'
    ).select([
        'numero_ruc', 'razon_social', 'estado_contribuyente',
        'tipo_contribuyente', 'clase_contribuyente', 'categoria',
        'obligado_llevar_contabilidad', 'agente_retencion', 'contribuyente_especial',
        'CODIGO', 'actividad_economica', 'estado_establecimiento',
        'porcentaje_retencion_iva', 'motivo_iva',
        'codigo_sri_renta', 'porcentaje_renta', 'base_calculo_renta',
        'nombre_fantasia_comercial',
        'numero_establecimiento', 'id_establecimiento',
        'fecha_inicio_actividades_comercio', 'fecha_actualizacion_comercio',
        'fecha_cese_comercio', 'fecha_reinicio_actividades_comercio',
        'direccion_completa', 'motivo_cancelacion_suspension',
        'contribuyente_fantasma', 'transacciones_inexistente',
        'nombre_representante_legal', 'identificacion_representante_legal',
        'representantes_legales',
    ])

    # Mapeo campo formulario 104 IVA
    campo_formulario_iva_map = {10: 721, 20: 723, 30: 725, 50: 727, 70: 729, 100: 731}
    iva_renta = iva_renta.with_columns(
        pl.col('porcentaje_retencion_iva')
        .replace(campo_formulario_iva_map, default=0)
        .alias('campo_formulario_104_iva')
    )

    # Cruce tabla retenciones → campo formulario 103 IR
    tabla_retenciones = (
        pl.read_excel(r"..\bases_datos\tabla_retenciones.xlsx")
        .select([
            pl.col(r'casillero Formulario 103  base imponible ').alias('campo_formulario_103_ir'),
            pl.col('Código del Anexo _1').cast(pl.Utf8).alias('codigo_anexo_ir'),
        ])
    )

    iva_renta_final = iva_renta.join(
        tabla_retenciones,
        left_on='codigo_sri_renta',
        right_on='codigo_anexo_ir',
        how='left'
    ).rename({'codigo_sri_renta': 'codigo_anexo_ir'})

    # Formatear para RDS
    df_final = formatear_para_rds(iva_renta_final)

    # =====================
    # Validaciones
    # =====================
    print("\n" + "=" * 60)
    print("RESULTADO FINAL")
    print("=" * 60)
    print(f"Registros: {len(df_final)}")
    print(f"Columnas ({len(df_final.columns)}): {df_final.columns}")
    print(f"\nTipos:")
    for col, dtype in zip(df_final.columns, df_final.dtypes):
        print(f"  {col:<40} {dtype}")

    print(f"\nNulos por columna:")
    for col in df_final.columns:
        nulos = df_final[col].null_count()
        if nulos > 0:
            print(f"  {col:<40} {nulos}")

    cols_tipo1 = [
        'numero_ruc_str', 'razon_social', 'provincia_jurisdiccion',
        'nombre_comercial', 'estado_contribuyente', 'clase_contribuyente',
        'fecha_inicio_actividades', 'fecha_actualizacion', 'fecha_suspension_definitiva',
        'fecha_reinicio_actividades', 'obligado', 'tipo_contribuyente',
        'numero_establecimiento', 'nombre_fantasia_comercial',
        'estado_establecimiento', 'descripcion_provincia_est',
        'descripcion_canton_est', 'descripcion_parroquia_est', 'codigo_ciiu',
        'actividad_economica', 'provincia_archivo_procesamiento',
        'numero_ruc', 'cedula', 'cedula_str',
        'id_establecimiento', 'categoria',
        'nro_campo', 'porcentaje_retencion_renta', 'porcentaje_retencion_iva',
        'codigo_anexo_ir', 'campo_formulario_104_iva',
        'codigo_anexo_iva', 'campo_formulario_103_ir', 'fecha_carga'
    ]

    cols_tipo0_extra = [
        'agente_retencion', 'contribuyente_especial', 'motivo_cancelacion_suspension',
        'contribuyente_fantasma', 'transacciones_inexistente',
        'nombre_representante_legal', 'representantes_legales',
    ]

    faltantes_tipo1 = [c for c in cols_tipo1 if c not in df_final.columns]
    faltantes_tipo0 = [c for c in cols_tipo1 + cols_tipo0_extra if c not in df_final.columns]

    print(f"\n--- Validación columnas tipo=1 (AVATI) ---")
    if faltantes_tipo1:
        print(f"  FALTAN: {faltantes_tipo1}")
    else:
        print(f"  OK — Todas las {len(cols_tipo1)} columnas presentes")

    print(f"\n--- Validación columnas tipo=0 (QPH) ---")
    if faltantes_tipo0:
        print(f"  FALTAN: {faltantes_tipo0}")
    else:
        print(f"  OK — Todas las {len(cols_tipo1) + len(cols_tipo0_extra)} columnas presentes")

    # Guardar resultado
    df_final.write_parquet(r"..\bases_datos\test_resultado_final.parquet")
    print(f"\nResultado guardado en: ../bases_datos/test_resultado_final.parquet")

    print(f"\nPrimeras 5 filas:")
    print(df_final.head(5))


if __name__ == "__main__":
    main()
