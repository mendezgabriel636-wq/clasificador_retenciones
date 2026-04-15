from sqlalchemy.engine import Engine

from app.Negocio.qph.automatizacion_impositivas.rds import RDS

# CAMBIO: Se reemplaza PROCESAR por calcular_retenciones (nuevo flujo)
from app.Negocio.qph.automatizacion_impositivas.calculo_retenciones import (
    calcular_retenciones,
)
from app.Utils.logger_config import LoggerManager

logger = LoggerManager(__name__)


def automatizar_impositivas(
    engine_data_fact: Engine, engine_data_fact_escritura: Engine
):

    logger.info("Proceso iniciado...")
    rds = RDS()

    # 1. Calcular retenciones (procesamiento + IVA + Renta + formateo RDS)
    ##################################################
    try:
        df_polars = calcular_retenciones(engine_data_fact)
    except Exception as e:
        logger.error(f"[automatizar_impositivas] Fallo en calcular_retenciones: {e}", exc_info=True)
        raise

    # CAMBIO: Convertir Polars → Pandas (rds.carga_base_retenciones espera Pandas)
    try:
        df = df_polars.to_pandas()
    except Exception as e:
        logger.error(f"[automatizar_impositivas] Fallo al convertir Polars → Pandas: {e}", exc_info=True)
        raise

    # 2. Cargar base_retenciones al RDS
    ##################################################
    logger.info("Cargando...")
    try:
        rds.carga_base_retenciones(
            df,
            engine_data_fact,
            engine_data_fact_escritura,
            table_name="base_rucs_retenciones_pruebas",
            schema="data_fact",
            tipo=1,
        )
        # rds.carga_base_retenciones(df, engine_data_fact, engine_data_fact_escritura, table_name = 'base_rucs_retenciones',schema = 'data_qph',tipo=0)
    except Exception as e:
        logger.error(f"[automatizar_impositivas] Fallo en carga_base_retenciones: {e}", exc_info=True)
        raise

    logger.info("Proceso terminado")
