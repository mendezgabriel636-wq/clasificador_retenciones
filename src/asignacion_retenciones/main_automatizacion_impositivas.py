import polars as pl
from .calculo_retenciones import (
    calcular_retenciones,
)
from .db_config_data_fact import (
    engine_data_fact_readonly,
    engine_data_fact_escritura,
)
from sqlalchemy import text, exc
from .rds import crear_tabla_sql
from .logger_config import LoggerManager

logger = LoggerManager(__name__)


def leer_reglas_retencion_renta() -> pl.DataFrame:
    try:
        base_rucs_sri = pl.read_database(
            f"SELECT * FROM reglas_retencion_renta",
            connection=engine_data_fact_readonly,
        )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.reglas_retencion_renta] {e}")
    return base_rucs_sri


def leer_reglas_retencion_iva() -> pl.DataFrame:
    try:
        base_rucs_sri = pl.read_database(
            f"SELECT * FROM reglas_retencion_iva",
            connection=engine_data_fact_readonly,
        )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.reglas_retencion_iva] {e}")
    return base_rucs_sri


def leer_base_rucs_sri() -> pl.DataFrame:
    try:
        base_rucs_sri = pl.read_database(
            f"SELECT * FROM base_rucs_sri WHERE matriz = 1",
            connection=engine_data_fact_readonly,
        )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.base_rucs_sri] {e}")
    return base_rucs_sri


def leer_ciiu_clasificado() -> pl.DataFrame:
    try:
        ciiu_clasificado = pl.read_database(
            f"SELECT * FROM ciiu_clasificado", connection=engine_data_fact_readonly
        )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.ciiu_clasificado] {e}")
    return ciiu_clasificado


def carga_base_retenciones(df: pl.DataFrame):
    separacion = 1000000
    full_table_name = f"data_fact.base_retenciones"

    with engine_data_fact_escritura.begin() as conn:
        conn.execute(
            text(
                f"CREATE TABLE {full_table_name}.backup AS SELECT * FROM {full_table_name}"
            )
        )
        conn.execute(text(f"DROP TABLE IF EXISTS {full_table_name}"))
        conn.execute(text(crear_tabla_sql))
        try:
            for i in range(0, len(df), separacion):
                bloque = df[i : i + separacion]
                try:
                    bloque.write_database(
                        table_name=full_table_name,
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
