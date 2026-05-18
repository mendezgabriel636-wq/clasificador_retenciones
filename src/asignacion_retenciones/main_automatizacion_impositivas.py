import polars as pl
from .calculo_retenciones import (
    calcular_retenciones,
)
from .db_config_data_fact import engine_data_fact_readonly
from .db_config_data_fact import engine_data_fact_escritura
from sqlalchemy import text, exc
from .rds import crear_tabla_sql
from .logger_config import LoggerManager

logger = LoggerManager(__name__)


def leer_reglas_retencion_renta(limit: int | None = None) -> pl.DataFrame:
    try:
        if limit is None:
            df = pl.read_database(
                "SELECT * FROM reglas_retencion_renta",
                connection=engine_data_fact_readonly,
            )
        else:
            df = pl.read_database(
                f"SELECT * FROM reglas_retencion_renta LIMIT {limit}",
                connection=engine_data_fact_readonly,
            )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.reglas_retencion_renta] {e}")
    return df


def leer_reglas_retencion_iva(limit: int | None = None) -> pl.DataFrame:
    try:
        if limit is None:
            df = pl.read_database(
                "SELECT * FROM reglas_retencion_iva",
                connection=engine_data_fact_readonly,
            )
        else:
            df = pl.read_database(
                f"SELECT * FROM reglas_retencion_iva LIMIT {limit}",
                connection=engine_data_fact_readonly,
            )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.reglas_retencion_iva] {e}")
    return df


def leer_base_rucs_sri(limit: int | None = None) -> pl.DataFrame:
    base = """
        SELECT
            numero_ruc,
            numero_establecimiento          AS numero_establecimiento_matriz,
            razon_social,
            estado_contribuyente,
            clase_contribuyente,
            CASE
                WHEN clase_contribuyente = 'RIMPE' AND (categoria IS NULL OR categoria = '')
                THEN 'EMPRENDEDOR'
                ELSE categoria
            END                                             AS categoria,
            obligado_llevar_contabilidad,
            agente_retencion,
            contribuyente_especial,
            contribuyente_fantasma,
            tipo_contribuyente,
            actividad_economica,
            CURDATE()                                   AS fecha_carga,
            fecha_inicio_actividades_comercio           AS fecha_inicio_actividades,
            fecha_cese_comercio                         AS fecha_suspension_definitiva,
            fecha_reinicio_actividades_comercio         AS fecha_reinicio_actividades,
            fecha_actualizacion_comercio                AS fecha_actualizacion
        FROM base_rucs_sri
        WHERE matriz = 1
        AND NULLIF(actividad_economica,'') IS NOT NULL
    """
    try:
        if limit is None:
            df = pl.read_database(base, connection=engine_data_fact_readonly)
        else:
            df = pl.read_database(
                base + f"\nLIMIT {limit}", connection=engine_data_fact_readonly
            )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.base_rucs_sri] {e}")
    return df


def leer_ciiu_clasificado(limit: int | None = None) -> pl.DataFrame:
    try:
        if limit is None:
            df = pl.read_database(
                "SELECT * FROM actividades_economicas_clasificadas",
                connection=engine_data_fact_readonly,
                infer_schema_length=None,
            )
        else:
            df = pl.read_database(
                f"SELECT * FROM actividades_economicas_clasificadas LIMIT {limit}",
                connection=engine_data_fact_readonly,
                infer_schema_length=None,
            )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(
            f"[obtener_informacion.actividades_economicas_clasificadas] {e}"
        )
    return df


def leer_base_rucs_catastro(limit: int | None = None) -> pl.DataFrame:
    try:
        if limit is None:
            df = pl.read_database(
                "SELECT * FROM base_rucs_catastro",
                connection=engine_data_fact_readonly,
            )
        else:
            df = pl.read_database(
                f"SELECT * FROM base_rucs_catastro LIMIT {limit}",
                connection=engine_data_fact_readonly,
            )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.base_rucs_catastro] {e}")
    return df


def leer_ciiu_nivel6(limit: int | None = None) -> pl.DataFrame:
    try:
        if limit is None:
            df = pl.read_database(
                "SELECT * FROM ciiu_nivel6",
                connection=engine_data_fact_readonly,
            )
        else:
            df = pl.read_database(
                f"SELECT * FROM ciiu_nivel6 LIMIT {limit}",
                connection=engine_data_fact_readonly,
            )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.ciiu_nivel6] {e}")
    return df


def leer_actividades_economicas_faltantes(limit: int | None = None) -> pl.DataFrame:
    try:
        if limit is None:
            df = pl.read_database(
                "SELECT * FROM actividades_economicas_faltantes",
                connection=engine_data_fact_readonly,
            )
        else:
            df = pl.read_database(
                f"SELECT * FROM actividades_economicas_faltantes LIMIT {limit}",
                connection=engine_data_fact_readonly,
            )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(
            f"[obtener_informacion.actividades_economicas_faltantes] {e}"
        )
    return df


def leer_rucs_bendo(limit: int | None = None) -> pl.DataFrame:
    try:
        if limit is None:
            df = pl.read_database(
                "SELECT * FROM rucs_bendo",
                connection=engine_data_fact_readonly,
            )
        else:
            df = pl.read_database(
                f"SELECT * FROM rucs_bendo LIMIT {limit}",
                connection=engine_data_fact_readonly,
            )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.rucs_bendo] {e}")
    return df


def carga_base_retenciones(df: pl.DataFrame):
    separacion = 2000000
    db = "data_fact"
    tabla = "base_retenciones"
    tabla_backup = "base_retenciones_backup"

    with engine_data_fact_escritura.begin() as conn:
        tabla_existe = conn.execute(
            text(
                "SELECT COUNT(*) FROM information_schema.tables "
                f"WHERE table_schema = '{db}' AND table_name = '{tabla}'"
            )
        ).scalar()
        if tabla_existe:
            conn.execute(text(f"DROP TABLE IF EXISTS `{db}`.`{tabla_backup}`"))
            conn.execute(
                text(
                    f"CREATE TABLE `{db}`.`{tabla_backup}` AS SELECT * FROM `{db}`.`{tabla}`"
                )
            )
        conn.execute(text(f"DROP TABLE IF EXISTS `{db}`.`{tabla}`"))
        conn.execute(text(crear_tabla_sql))
        try:
            for i in range(0, len(df), separacion):
                bloque = df[i : i + separacion]
                try:
                    bloque.write_database(
                        table_name=f"{db}.{tabla}",
                        connection=conn,
                        if_table_exists="append",
                    )
                except exc.SQLAlchemyError as e:
                    raise RuntimeError(
                        f"[carga_base_retenciones(data_fact)] bloque falló — {str(e)[::150]}"
                    )
        except Exception as e:
            logger.error(f"{e}")
            raise

    logger.info("Carga al RDS terminada correctamente")


def automatizar_impositivas():
    try:
        base_rucs_sri = leer_base_rucs_sri()
        ciiu_clasificado = leer_ciiu_clasificado()
        reglas_retenciones_renta = leer_reglas_retencion_renta()
        reglas_retenciones_iva = leer_reglas_retencion_iva()
        df_retenciones = calcular_retenciones(
            base_rucs_sri=base_rucs_sri,
            ciiu_clasificado=ciiu_clasificado,
            reglas_retenciones_iva=reglas_retenciones_iva,
            reglas_retenciones_renta=reglas_retenciones_renta,
        )
        carga_base_retenciones(df_retenciones)
    except Exception as e:
        logger.error(f"{e}")
        raise
