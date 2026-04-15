from sqlalchemy.engine import Engine

from app.Negocio.qph.automatizacion_impositivas.rds import RDS
# CAMBIO: Se reemplaza PROCESAR por calcular_retenciones (nuevo flujo)
from app.Negocio.qph.automatizacion_impositivas.calculo_retenciones import calcular_retenciones
from app.Utils.logger_config import LoggerManager

logger = LoggerManager(__name__)

def automatizar_impositivas(engine_data_fact: Engine, engine_data_fact_escritura: Engine):

    logger.info('Proceso iniciado...')
    rds = RDS()

    # 1. Calcular retenciones (procesamiento + IVA + Renta + formateo RDS)
    # CAMBIO: Reemplaza el flujo anterior de leer_base_sri → proceso1
    ##################################################
    df_polars = calcular_retenciones(engine_data_fact)

    # CAMBIO: Convertir Polars → Pandas (rds.carga_base_retenciones espera Pandas)
    df = df_polars.to_pandas()

    # 2. Cargar base_retenciones al RDS
    ##################################################
    logger.info('Cargando...')
    rds.carga_base_retenciones(df, engine_data_fact, engine_data_fact_escritura, table_name = 'base_rucs_retenciones_pruebas',schema = 'data_fact',tipo=1)
    #rds.carga_base_retenciones(df, engine_data_fact, engine_data_fact_escritura, table_name = 'base_rucs_retenciones',schema = 'data_qph',tipo=0)

    logger.info('Proceso terminado')
