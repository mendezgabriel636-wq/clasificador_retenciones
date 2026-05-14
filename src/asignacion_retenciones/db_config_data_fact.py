from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# load the environment variables
load_dotenv()

DB_USER_DATA_FACT_READONLY = os.getenv("DB_USER_DATA_FACT")
DB_PASS_DATA_FACT_READONLY = os.getenv("DB_PASSWORD_DATA_FACT")
DB_NAME_DATA_FACT_READONLY = os.getenv("DB_NAME_DATA_FACT")
DB_HOST_DATA_FACT_READONLY = os.getenv("DB_HOST_DATA_FACT")
DB_PORT_DATA_FACT_READONLY = os.getenv("DB_PORT_DATA_FACT")

assert (
    DB_USER_DATA_FACT_READONLY
    and DB_PASS_DATA_FACT_READONLY
    and DB_NAME_DATA_FACT_READONLY
    and DB_HOST_DATA_FACT_READONLY
    and DB_PORT_DATA_FACT_READONLY
)


db_connection_str = (
    "mysql+pymysql://"
    + DB_USER_DATA_FACT_READONLY
    + ":"
    + DB_PASS_DATA_FACT_READONLY
    + "@"
    + DB_HOST_DATA_FACT_READONLY
    + ":"
    + DB_PORT_DATA_FACT_READONLY
    + "/"
    + DB_NAME_DATA_FACT_READONLY
)
engine_data_fact_readonly = create_engine(
    db_connection_str, pool_pre_ping=True, pool_recycle=27000
)


DB_USER_DATA_FACT_ESCRIT = os.getenv("DB_USER_DATA_FACT_ESCRIT")
DB_PASS_DATA_FACT_ESCRIT = os.getenv("DB_PASSWORD_DATA_FACT_ESCRIT")
DB_NAME_DATA_FACT_ESCRIT = os.getenv("DB_NAME_DATA_FACT_ESCRIT")
DB_HOST_DATA_FACT_ESCRIT = os.getenv("DB_HOST_DATA_FACT_ESCRIT")
DB_PORT_DATA_FACT_ESCRIT = os.getenv("DB_PORT_DATA_FACT_ESCRIT")


assert (
    DB_USER_DATA_FACT_ESCRIT
    and DB_PASS_DATA_FACT_ESCRIT
    and DB_NAME_DATA_FACT_ESCRIT
    and DB_HOST_DATA_FACT_ESCRIT
    and DB_PORT_DATA_FACT_ESCRIT
)

db_connection_str_data_fact_escrit = (
    "mysql+pymysql://"
    + DB_USER_DATA_FACT_ESCRIT
    + ":"
    + DB_PASS_DATA_FACT_ESCRIT
    + "@"
    + DB_HOST_DATA_FACT_ESCRIT
    + ":"
    + DB_PORT_DATA_FACT_ESCRIT
    + "/"
    + DB_NAME_DATA_FACT_ESCRIT
)
# Create the database engine
engine_data_fact_escritura = create_engine(
    db_connection_str_data_fact_escrit, pool_pre_ping=True, pool_recycle=27000
)
