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


def leer_base_rucs_sri(nombre_tabla: str) -> pl.DataFrame:
    try:
        base_rucs_sri = pl.read_database(
            f"SELECT * FROM {nombre_tabla} WHERE matriz = 1",
            connection=engine_data_fact_readonly,
        )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.base_rucs_sri] {e}")
    return base_rucs_sri


def leer_ciiu_clasificado(nombre_tabla: str) -> pl.DataFrame:
    try:
        ciiu_clasificado = pl.read_database(
            f"SELECT * FROM {nombre_tabla}", connection=engine_data_fact_readonly
        )
    except exc.SQLAlchemyError as e:
        raise RuntimeError(f"[obtener_informacion.ciiu_clasificado] {e}")
    return ciiu_clasificado


def carga_base_retenciones(df: pl.DataFrame):
    separacion = 1000000
    full_table_name = f"data_fact.base_retenciones"

    with engine_data_fact_escritura.begin() as conn:
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
        base_rucs_sri = leer_base_rucs_sri("base_rucs_sri")
        ciiu_clasificado = leer_ciiu_clasificado("ciiu_clasificado")
        df_retenciones = calcular_retenciones(
            base_rucs_sri=base_rucs_sri, ciiu_clasificado=ciiu_clasificado
        )
        carga_base_retenciones(df_retenciones)
    except Exception as e:
        logger.error(f"{e}")
        raise
