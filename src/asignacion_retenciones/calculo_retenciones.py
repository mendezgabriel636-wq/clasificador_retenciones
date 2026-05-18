import polars as pl
from polars import exceptions as plexec


def aplicar_retencion_iva(
    tb: pl.DataFrame, reglas_retenciones_iva: pl.DataFrame
) -> pl.DataFrame:
    # Claves = columnas que existen en ambas tablas; el resto (porcentajes/campos)
    # son columnas de salida que se añaden al resultado desde la tabla de reglas.
    cols_tb = set(tb.columns)
    claves = [c for c in reglas_retenciones_iva.columns if c in cols_tb]
    if not claves:
        raise ValueError(
            "[calcular_retenciones.retencion_iva] Ninguna columna de reglas_retenciones_iva "
            "existe en la tabla base. Verifica las claves de clasificación."
        )
    try:
        resultado = tb.join(reglas_retenciones_iva, on=claves, how="left")
        if "porcentaje_retencion_iva" not in resultado.columns:
            raise ValueError(
                "[calcular_retenciones.retencion_iva] Falta columna 'porcentaje_retencion_iva' "
                "en reglas_retenciones_iva."
            )
    except plexec.ColumnNotFoundError as e:
        raise ValueError(f"[calcular_retenciones.retencion_iva] {e}")
    except Exception:
        raise
    return resultado


def aplicar_retencion_renta(
    tb: pl.DataFrame, reglas_retenciones_renta: pl.DataFrame
) -> pl.DataFrame:
    cols_tb = set(tb.columns)
    claves = [c for c in reglas_retenciones_renta.columns if c in cols_tb]
    if not claves:
        raise ValueError(
            "[calcular_retenciones.retencion_renta] Ninguna columna de reglas_retenciones_renta "
            "existe en la tabla base. Verifica las claves de clasificación."
        )
    try:
        resultado = tb.join(reglas_retenciones_renta, on=claves, how="left")
        if "porcentaje_retencion_renta" not in resultado.columns:
            raise ValueError(
                "[calcular_retenciones.retencion_renta] Falta columna 'porcentaje_retencion_renta' "
                "en reglas_retenciones_renta."
            )
    except plexec.ColumnNotFoundError as e:
        raise ValueError(f"[calcular_retenciones.retencion_renta] {e}")
    except Exception:
        raise
    return resultado


def formateo(tb: pl.DataFrame) -> pl.DataFrame:
    if "obligado_llevar_contabilidad" in tb.columns and "obligado" not in tb.columns:
        tb = tb.rename({"obligado_llevar_contabilidad": "obligado"})

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


def _log_nulls(tb: pl.DataFrame, columnas: list[str], etapa: str) -> None:
    for col in columnas:
        if col not in tb.columns:
            continue
        n_null = tb[col].null_count()
        if n_null:
            total = len(tb)
            ejemplos = (
                tb.filter(pl.col(col).is_null())
                .select(["actividad_economica", "clase_contribuyente", "categoria"])
                .head(3)
                .to_dicts()
            )
            raise RuntimeError(
                f"[{etapa}] '{col}' tiene {n_null:,}/{total:,} filas sin valor.\n"
                f"  Ejemplos sin match:\n"
                + "".join(f"    - {e}\n" for e in ejemplos)
                + f"  Causa probable: actividad_economica no clasificada o combinación "
                f"clase/categoría/tipo_concepto sin regla definida."
            )


def _aplicar_defaults_renta(tb: pl.DataFrame) -> pl.DataFrame:
    """Rellena retención de renta para filas que no matchearon la tabla de reglas.
    Caso conocido: RIMPE sin categoría asignada (959 filas) → trata como Emprendedor."""
    sin = pl.col("porcentaje_retencion_renta").is_null()
    es_rimpe = pl.col("clase_contribuyente") == "RIMPE"
    es_especial = (pl.col("contribuyente_especial") == 1) | (pl.col("clase_contribuyente") == "ESPECIAL")

    return tb.with_columns([
        pl.when(~sin).then(pl.col("porcentaje_retencion_renta"))
          .when(es_especial).then(pl.lit(0.0).cast(pl.Float32))
          .when(es_rimpe).then(pl.lit(1.0).cast(pl.Float32))   # RIMPE sin categoría → Emprendedor
          .otherwise(pl.lit(3.0).cast(pl.Float32))             # residual
          .alias("porcentaje_retencion_renta"),

        pl.when(~sin).then(pl.col("campo_formulario_103_ir"))
          .when(es_especial).then(pl.lit(332).cast(pl.Int32))
          .when(es_rimpe).then(pl.lit(343).cast(pl.Int32))
          .otherwise(pl.lit(3440).cast(pl.Int32))
          .alias("campo_formulario_103_ir"),

        pl.when(~sin).then(pl.col("codigo_anexo_ir"))
          .when(es_especial).then(pl.lit(3321).cast(pl.Int32))
          .when(es_rimpe).then(pl.lit(3431).cast(pl.Int32))
          .otherwise(pl.lit(34402).cast(pl.Int32))
          .alias("codigo_anexo_ir"),

        pl.when(~sin).then(pl.col("nro_campo"))
          .when(es_especial).then(pl.lit(1).cast(pl.Int32))
          .when(es_rimpe).then(pl.lit(7).cast(pl.Int32))
          .otherwise(pl.lit(29).cast(pl.Int32))
          .alias("nro_campo"),
    ])


def calcular_retenciones(
    base_rucs_sri: pl.DataFrame,
    ciiu_clasificado: pl.DataFrame,
    reglas_retenciones_iva: pl.DataFrame,
    reglas_retenciones_renta: pl.DataFrame,
) -> pl.DataFrame:
    try:
        base_rucs_corregida_clasificada = base_rucs_sri.join(
            ciiu_clasificado, on=["actividad_economica"], how="left"
        )
        sin_ciiu = base_rucs_corregida_clasificada["tipo_concepto_ir"].null_count()
        if sin_ciiu:
            total = len(base_rucs_corregida_clasificada)
            raise RuntimeError(
                f"[calcular_retenciones.ciiu] {sin_ciiu:,}/{total:,} actividades sin clasificar "
                f"en ciiu_clasificado — agrégalas a actividades_economicas_clasificadas."
            )
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"[calcular_retenciones.asignar_metadatos] {e}")

    try:
        aplicado_iva = aplicar_retencion_iva(
            base_rucs_corregida_clasificada, reglas_retenciones_iva
        )
        _log_nulls(aplicado_iva, ["porcentaje_retencion_iva"], "retencion_iva")

        aplicado_iva_renta = aplicar_retencion_renta(
            aplicado_iva, reglas_retenciones_renta
        )
        _log_nulls(
            aplicado_iva_renta,
            ["porcentaje_retencion_renta", "nro_campo"],
            "retencion_renta",
        )

        tabla_resultado = formateo(aplicado_iva_renta)
    except RuntimeError:
        raise
    except Exception as e:
        raise RuntimeError(f"{e}")

    return tabla_resultado
