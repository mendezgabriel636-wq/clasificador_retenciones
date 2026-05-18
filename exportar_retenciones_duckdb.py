"""
Lee base_retenciones de MySQL y la guarda en catalogo_retenciones.duckdb.

Uso:
    uv run exportar_retenciones_duckdb.py

La tabla en DuckDB se llama igual: base_retenciones.
Si ya existe, se reemplaza.
"""
import os

import duckdb
import polars as pl
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

DUCKDB_PATH = "catalogo_retenciones.duckdb"
TABLA = "base_retenciones"
CHUNK = 500_000


def _get_engine():
    u = os.getenv("DB_USER_DATA_FACT")
    p = os.getenv("DB_PASSWORD_DATA_FACT")
    h = os.getenv("DB_HOST_DATA_FACT")
    port = os.getenv("DB_PORT_DATA_FACT")
    db = os.getenv("DB_NAME_DATA_FACT")
    assert u and p and h and port and db, "Faltan variables DB_* en .env"
    return create_engine(
        f"mysql+pymysql://{u}:{p}@{h}:{port}/{db}",
        pool_pre_ping=True,
        pool_recycle=27000,
    )


def main():
    engine = _get_engine()

    print(f"Leyendo '{TABLA}' desde MySQL...")
    total = engine.connect().execute(
        __import__("sqlalchemy").text(f"SELECT COUNT(*) FROM {TABLA}")
    ).scalar()
    print(f"  {total:,} filas encontradas")

    con = duckdb.connect(DUCKDB_PATH)
    con.execute(f"DROP TABLE IF EXISTS {TABLA}")

    filas_cargadas = 0
    primera = True

    for offset in range(0, max(total, 1), CHUNK):
        df = pl.read_database(
            f"SELECT * FROM {TABLA} LIMIT {CHUNK} OFFSET {offset}",
            connection=engine,
        )

        if primera:
            con.execute(f"CREATE TABLE {TABLA} AS SELECT * FROM df")
            primera = False
        else:
            con.execute(f"INSERT INTO {TABLA} SELECT * FROM df")

        filas_cargadas += len(df)
        print(f"  {filas_cargadas:,}/{total:,}")

    con.close()
    print(f"\nListo. '{TABLA}' guardada en {DUCKDB_PATH}")


if __name__ == "__main__":
    main()
