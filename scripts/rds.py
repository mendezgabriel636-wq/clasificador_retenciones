# rds.py
# lo relacionado al RDS

import pandas as pd
import numpy as np
import time
from typing import Optional

from sqlalchemy import inspect, text
from sqlalchemy.exc import SQLAlchemyError
import logging
from sqlalchemy.engine import Engine
from app.Utils.logger_config import LoggerManager

from sqlalchemy import (
    Table,
    Column,
    MetaData,
    String,
    BigInteger,
    SmallInteger,
    Date,
    DateTime,
    inspect,
    text,
    JSON,
)
from sqlalchemy.dialects.mysql import BIGINT, SMALLINT, TINYINT, DECIMAL

logger = LoggerManager(__name__)


def tipos(df):
    dtypes_cols = {
        "numero_establecimiento": np.int32,
        "numero_ruc": np.int64,
        "id_establecimiento": np.int64,
        # 'razon_social': str,
        # 'provincia_jurisdiccion': str,
        # 'nombre_comercial': str,
        # 'cod_estado_contribuyente': np.int32,
        # 'estado_contribuyente': str,
        # 'cod_clase_contribuyente': np.int32,
        # 'clase_contribuyente': str,
        # 'fecha_inicio_actividades': str,
        # 'fecha_actualizacion': str,
        # 'fecha_suspension_definitiva': str,
        # 'fecha_reinicio_actividades': str,
        # 'obligado': str,
        # 'cod_tipo_contribuyente': np.int32,
        # 'tipo_contribuyente': str,
        # 'nombre_fantasia_comercial': str,
        # 'cod_estado_establecimiento': np.int32,
        # 'estado_establecimiento': str,
        # 'descripcion_provincia_est': str,
        # 'descripcion_canton_est': str,
        # 'descripcion_parroquia_est': str,
        # 'codigo_ciiu': str,
        # 'categoria': str,
        # 'Número de campo': str,
        # '% Retención Renta': np.float64,
        # '% Retención IVA': np.float64,
        # 'actividad_economica': str,
    }

    df = df.astype(dtypes_cols)

    new_names = {
        "Número de campo": "nro_campo",
        "% Retención Renta": "porcentaje_retencion_renta",
        "% Retención IVA": "porcentaje_retencion_iva",
    }

    df.rename(columns=new_names, inplace=True)


class RDS:

    def leer_base_sri(self, engine_data_fact: Engine) -> Optional[pd.DataFrame]:
        consulta = f"""
        SELECT * FROM data_fact.base_rucs_sri
            """
        try:
            df = pd.read_sql(consulta, engine_data_fact=engine_data_fact)
            logger.info(f"Consulta exitosa. Total registros: {len(df)}")
            return df

        except SQLAlchemyError as e:
            logger.info(
                f"Error de SQLAlchemy al leer base_precalificados_onboarding: {e}"
            )
            return None

        except Exception as e:
            logger.info(f"Error inesperado: {e}")
            return None

    def carga_base_retenciones(
        self,
        df: pd.DataFrame,
        engine_data_fact: Engine,
        engine_data_fact_escritura: Engine,
        table_name: str = "base_rucs_retenciones_pruebas",
        schema: str = "data_fact",
        tipo: int = 0,
    ) -> None:
        separacion = 1000000
        intentos_max = 3
        full_table_name = f"{schema}.{table_name}"

        logger.info(f"Se van a colocar los datos en la tabla: {full_table_name}")

        if df is None or df.empty:
            raise ValueError("El DataFrame está vacío o no es válido")

        if tipo == 1:
            logger.info("Se usarán todas las columnas AVATI")
            columnas_requeridas = [
                "numero_ruc_str",
                "razon_social",
                "provincia_jurisdiccion",
                "nombre_comercial",
                "estado_contribuyente",
                "clase_contribuyente",
                "fecha_inicio_actividades",
                "fecha_actualizacion",
                "fecha_suspension_definitiva",
                "fecha_reinicio_actividades",
                "obligado",
                "tipo_contribuyente",
                "numero_establecimiento",
                "nombre_fantasia_comercial",
                "estado_establecimiento",
                "descripcion_provincia_est",
                "descripcion_canton_est",
                "descripcion_parroquia_est",
                "codigo_ciiu",
                "actividad_economica",
                "provincia_archivo_procesamiento",
                "numero_ruc",
                "cedula",
                "cedula_str",
                "id_establecimiento",
                "categoria",
                "nro_campo",
                "porcentaje_retencion_renta",
                "porcentaje_retencion_iva",
                "codigo_anexo_ir",
                "campo_formulario_104_iva",
                "codigo_anexo_iva",
                "campo_formulario_103_ir",
                "fecha_carga",
            ]

            missing = [col for col in columnas_requeridas if col not in df.columns]
            if missing:
                raise ValueError(f"No se cargarán: {missing}")
            df = df[columnas_requeridas]

        if tipo == 0:
            logger.info("Se usarán todas las columnas QPH")

        # ============================
        # CAMBIO
        if tipo == 0:
            import json
            import pandas as pd

            # if 'identificacion_representante_legal' not in df.columns:
            #     logger.warning('Advertencia: no existe columna identificacion_representante_legal; se usará vacío')
            #     df['identificacion_representante_legal'] = pd.NA
            # if 'nombre_representante_legal' not in df.columns:
            #     logger.warning('Advertencia: no existe columna nombre_representante_legal; se usará vacío')
            #     df['nombre_representante_legal'] = pd.NA

            # def _norm(x):
            #     if pd.isna(x):
            #         return None
            #     s = str(x).strip()
            #     return s if s else None

            # def _build_rep(row):
            #     nombre = _norm(row['nombre_representante_legal'])
            #     numero = _norm(row['identificacion_representante_legal'])
            #     if not nombre and not numero:
            #         return '[]'
            #     obj = {}
            #     if nombre:
            #         obj['nombre'] = nombre
            #     if numero:
            #         obj['numeroIdentificacion'] = numero
            #     return json.dumps([obj], ensure_ascii=False)

            # df['representante_legal'] = df.apply(_build_rep, axis=1)  # [NUEVO]
            if "representantes_legales" not in df.columns:
                logger.warning(
                    "Advertencia: no existe columna representantes_legales; se usará vacío"
                )
                df["representantes_legales"] = pd.NA

            df["representantes_legales"] = df["representantes_legales"].str.replace(
                '"identificacion"', '"numeroIdentificacion"', regex=False
            )

            columnas_requeridas_0 = [
                "numero_ruc_str",
                "razon_social",
                "provincia_jurisdiccion",
                "nombre_comercial",
                "estado_contribuyente",
                "clase_contribuyente",
                "fecha_inicio_actividades",
                "fecha_actualizacion",
                "fecha_suspension_definitiva",
                "fecha_reinicio_actividades",
                "obligado",
                "tipo_contribuyente",
                "numero_establecimiento",
                "nombre_fantasia_comercial",
                "estado_establecimiento",
                "descripcion_provincia_est",
                "descripcion_canton_est",
                "descripcion_parroquia_est",
                "codigo_ciiu",
                "actividad_economica",
                "provincia_archivo_procesamiento",
                "numero_ruc",
                "cedula",
                "cedula_str",
                "id_establecimiento",
                "categoria",
                "agente_retencion",
                "contribuyente_especial",
                "motivo_cancelacion_suspension",
                "contribuyente_fantasma",
                "transacciones_inexistente",
                "nombre_representante_legal",
                "representantes_legales",
                "nro_campo",
                "porcentaje_retencion_renta",
                "porcentaje_retencion_iva",
                "codigo_anexo_ir",
                "campo_formulario_104_iva",
                "codigo_anexo_iva",
                "campo_formulario_103_ir",
                "fecha_carga",
            ]
            faltan = [c for c in columnas_requeridas_0 if c not in df.columns]
            for c in faltan:
                df[c] = pd.NA
            df = df[columnas_requeridas_0]
        # ============================

        metadata = MetaData(schema=schema)

        if tipo == 1:
            tabla_retenciones = Table(
                table_name,
                metadata,
                Column("numero_ruc_str", String(13)),
                Column("razon_social", String(974)),
                Column("provincia_jurisdiccion", String(36)),
                Column("nombre_comercial", String(300)),
                Column("estado_contribuyente", String(30)),
                Column("clase_contribuyente", String(30)),
                Column("fecha_inicio_actividades", Date),
                Column("fecha_actualizacion", DateTime),
                Column("fecha_suspension_definitiva", Date),
                Column("fecha_reinicio_actividades", Date),
                Column("obligado", TINYINT(1)),
                Column("tipo_contribuyente", String(30)),
                Column("numero_establecimiento", SMALLINT(unsigned=True)),
                Column("nombre_fantasia_comercial", String(300)),
                Column("estado_establecimiento", String(30)),
                Column("descripcion_provincia_est", String(36)),
                Column("descripcion_canton_est", String(32)),
                Column("descripcion_parroquia_est", String(70)),
                Column("codigo_ciiu", String(9)),
                Column("actividad_economica", String(1400)),
                Column("provincia_archivo_procesamiento", String(36)),
                Column("numero_ruc", DECIMAL(15, 0, unsigned=True)),
                Column("cedula", DECIMAL(15, 0, unsigned=True)),
                Column("cedula_str", String(13)),
                Column("id_establecimiento", DECIMAL(18, 0, unsigned=True)),
                Column("categoria", String(30)),
                Column("nro_campo", String(30)),
                Column("porcentaje_retencion_renta", String(30)),
                Column("porcentaje_retencion_iva", String(30)),
                Column("codigo_anexo_ir", String(10)),
                Column("campo_formulario_104_iva", String(50)),
                Column("codigo_anexo_iva", String(50)),
                Column("campo_formulario_103_ir", String(10)),
                Column("fecha_carga", DateTime),
            )
        if tipo == 0:

            tabla_retenciones = Table(
                table_name,
                metadata,
                Column("numero_ruc_str", String),
                Column("razon_social", String),
                Column("provincia_jurisdiccion", String),
                Column("nombre_comercial", String),
                Column("estado_contribuyente", String),
                Column("clase_contribuyente", String),
                Column("fecha_inicio_actividades", Date),
                Column("fecha_actualizacion", DateTime),
                Column("fecha_suspension_definitiva", Date),
                Column("fecha_reinicio_actividades", Date),
                Column("obligado", TINYINT(1)),
                Column("tipo_contribuyente", String),
                Column("numero_establecimiento", SMALLINT(unsigned=True)),
                Column("nombre_fantasia_comercial", String),
                Column("estado_establecimiento", String),
                Column("descripcion_provincia_est", String),
                Column("descripcion_canton_est", String),
                Column("descripcion_parroquia_est", String),
                Column("codigo_ciiu", String),
                Column("actividad_economica", String),
                Column("provincia_archivo_procesamiento", String),
                Column("numero_ruc", DECIMAL(15, 0, unsigned=True)),
                Column("cedula", DECIMAL(15, 0, unsigned=True)),
                Column("cedula_str", String(13)),
                Column("id_establecimiento", DECIMAL(18, 0, unsigned=True)),
                Column("categoria", String),
                Column("agente_retencion", TINYINT(unsigned=True)),
                Column("contribuyente_especial", TINYINT(unsigned=True)),
                Column("motivo_cancelacion_suspension", String(255)),
                Column("contribuyente_fantasma", TINYINT(unsigned=True)),
                Column("transacciones_inexistente", TINYINT(unsigned=True)),
                Column("nombre_representante_legal", String),
                Column("representantes_legales", JSON),
                Column("nro_campo", String),
                Column("porcentaje_retencion_renta", String),
                Column("porcentaje_retencion_iva", String),
                Column("codigo_anexo_ir", String),
                Column("campo_formulario_104_iva", String),
                Column("codigo_anexo_iva", String),
                Column("campo_formulario_103_ir", String),
                Column("fecha_carga", DateTime),
            )

        inspector = inspect(engine_data_fact)
        tabla_existe = inspector.has_table(table_name, schema=schema)

        if tabla_existe:
            logger.info(f"La tabla {table_name} existe. Verificando compatibilidad de columnas...")
            db_cols_info = {
                col["name"].lower(): getattr(col["type"], "length", None)
                for col in inspector.get_columns(table_name, schema=schema)
            }
            def_cols_info = {
                col.name.lower(): getattr(col.type, "length", None)
                for col in tabla_retenciones.columns
            }

            mismatches = {
                col: (db_cols_info.get(col), def_cols_info.get(col))
                for col in def_cols_info
                if db_cols_info.get(col) != def_cols_info.get(col)
            }
            if mismatches:
                logger.info(f"Diferencias de esquema detectadas: {mismatches}")

            esquemas_coinciden = db_cols_info == def_cols_info

            if esquemas_coinciden:
                try:
                    with engine_data_fact_escritura.begin() as connection:
                        connection.execute(text(f"TRUNCATE TABLE {full_table_name}"))
                    logger.info("truncate ok")
                except SQLAlchemyError as e:
                    logging.error(
                        "Error al truncar la tabla '%s': %s", full_table_name, e
                    )
                    raise
            else:
                logger.info(
                    "La estructura no coincide. Se eliminará y recreará la tabla..."
                )
                try:
                    with engine_data_fact_escritura.begin() as connection:
                        connection.execute(text(f"DROP TABLE {full_table_name}"))
                    logger.info("Tabla eliminada correctamente.")
                    tabla_retenciones.create(bind=engine_data_fact_escritura)
                    logger.info("Tabla recreada con estructura definida.")
                except SQLAlchemyError as e:
                    logging.error(
                        "Error al recrear la tabla '%s': %s", full_table_name, e
                    )
                    raise
        else:
            logger.info(
                f"La tabla {full_table_name} no existe. Se va a crear con la estructura definida..."
            )
            try:
                tabla_retenciones.create(bind=engine_data_fact_escritura)
                logger.info("Tabla creada correctamente.")
            except SQLAlchemyError as e:
                logging.error("Error al crear la tabla '%s': %s", full_table_name, e)
                raise

        # Auditoría previa: loguea valores de texto que superen 300 caracteres
        cols_texto = df.select_dtypes(include="object").columns
        for col in cols_texto:
            mask = df[col].astype(str).str.len() > 300
            if mask.any():
                ejemplos = df.loc[mask, col].head(3).tolist()
                logger.error(
                    f"[pre-insert] Columna '{col}' tiene {mask.sum()} valores con más de 300 chars. "
                    f"Ejemplos: {ejemplos}"
                )

        total_filas = len(df)
        for i in range(0, total_filas, separacion):
            bloque = df.iloc[i : i + separacion]
            intento = 1

            while intento <= intentos_max:
                try:
                    logger.info(
                        f"Insertando desde {i} hasta {i + len(bloque)} (Intento {intento})"
                    )
                    bloque.to_sql(
                        name=table_name,
                        con=engine_data_fact_escritura,
                        schema=schema,
                        if_exists="append",
                        index=False,
                    )
                    break
                except SQLAlchemyError as e:
                    logger.error(
                        f"[to_sql ERROR] Intento {intento}/{intentos_max} — bloque {i}-{i + len(bloque)}: {str(e)[:500]}",
                        exc_info=True,
                    )
                    time.sleep(5 * intento)
                    intento += 1
            else:
                msg = (
                    f"Fallo persistente al insertar bloque {i}-{i + len(bloque)} "
                    f"tras {intentos_max} intentos. Proceso abortado."
                )
                logger.error(msg)
                raise RuntimeError(msg)

        logger.info("Carga al RDS terminada correctamente")
