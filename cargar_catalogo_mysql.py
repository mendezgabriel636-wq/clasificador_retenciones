"""
Carga el catálogo DuckDB en MySQL.

Uso:
  uv run cargar_catalogo_mysql.py                            # carga todas las tablas
  uv run cargar_catalogo_mysql.py actividades_economicas_clasificadas base_rucs_sri

Nombres disponibles (nombre MySQL):
  actividades_economicas_clasificadas, ciiu_clasificado_por_codigo,
  base_rucs_sri, base_rucs_catastro,
  ciiu_nivel6, actividades_economicas_faltantes, rucs_bendo
"""
import os
import sys

import duckdb
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DUCKDB_PATH = "catalogo_retenciones.duckdb"
CHUNK = 500_000

# duckdb_nombre → mysql_nombre
CARGA = {
    "actividades_economicas_clasificadas": "actividades_economicas_clasificadas",
    "actividades_economicas_faltantes": "actividades_economicas_faltantes",
    "base_rucs_catastro": "base_rucs_catastro",
    "base_rucs_sri": "base_rucs_sri",
    "ciiu_clasificado": "ciiu_clasificado_por_codigo",
    "ciiu_nivel6": "ciiu_nivel6",
    "rucs_bendo": "rucs_bendo",
}


def _get_engine():
    u = os.getenv("DB_USER_DATA_FACT_ESCRIT") or os.getenv("DB_USER_DATA_FACT")
    p = os.getenv("DB_PASSWORD_DATA_FACT_ESCRIT") or os.getenv("DB_PASSWORD_DATA_FACT")
    h = os.getenv("DB_HOST_DATA_FACT_ESCRIT") or os.getenv("DB_HOST_DATA_FACT")
    port = os.getenv("DB_PORT_DATA_FACT_ESCRIT") or os.getenv("DB_PORT_DATA_FACT")
    db = os.getenv("DB_NAME_DATA_FACT_ESCRIT") or os.getenv("DB_NAME_DATA_FACT")
    assert u and p and h and port and db, (
        "Faltan credenciales de DB en .env (DB_USER_DATA_FACT o DB_USER_DATA_FACT_ESCRIT)"
    )
    return create_engine(
        f"mysql+pymysql://{u}:{p}@{h}:{port}/{db}",
        pool_pre_ping=True,
        pool_recycle=27000,
    )


def cargar_tabla(engine, duckdb_nombre, mysql_nombre):
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    n = con.execute(f"SELECT COUNT(*) FROM {duckdb_nombre}").fetchone()[0]
    print(f"\n  {duckdb_nombre} → {mysql_nombre}  ({n:,} filas)")

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS `{mysql_nombre}`"))

    for offset in range(0, max(n, 1), CHUNK):
        df = con.execute(
            f"SELECT * FROM {duckdb_nombre} LIMIT {CHUNK} OFFSET {offset}"
        ).pl()
        df.write_database(mysql_nombre, connection=engine, if_table_exists="append")
        cargadas = min(offset + CHUNK, n)
        print(f"    {cargadas:,}/{n:,}")

    con.close()
    print(f"  OK: {mysql_nombre}")


def main():
    # mysql_nombre → duckdb_nombre (invertido para lookup por argumento)
    por_mysql = {dst: src for src, dst in CARGA.items()}

    args = sys.argv[1:]
    if args:
        invalidos = [a for a in args if a not in por_mysql]
        if invalidos:
            print(f"Tablas desconocidas: {invalidos}")
            print(f"Disponibles: {sorted(por_mysql)}")
            sys.exit(1)
        seleccion = {por_mysql[a]: a for a in args}
    else:
        seleccion = CARGA

    engine = _get_engine()
    print("=== Cargando tablas desde DuckDB ===")
    for src, dst in seleccion.items():
        cargar_tabla(engine, src, dst)
    print("\n=== Listo ===")


if __name__ == "__main__":
    main()
