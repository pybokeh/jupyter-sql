"""
This library assumes you have the following environment variables set in a .env file:
- DB_HOST
- DB_USER
- DB_PASSWORD
- DB_NAME
- DB_PORT
- PG_JDBC_DRIVER_PATH (Optional)
"""

import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame

load_dotenv()

JDBC_OPTIONS = {
    "url": (
        f"jdbc:postgresql://{os.environ['DB_HOST']}:"
        f"{os.environ.get('DB_PORT', '5432')}/{os.environ['DB_NAME']}"
    ),
    "user": os.environ["DB_USER"],
    "password": os.environ["DB_PASSWORD"],
    "driver": "org.postgresql.Driver",
}

def get_spark(app_name: str = "Postgres") -> SparkSession:
    return (
        SparkSession.builder
        .config("spark.sql.repl.eagerEval.enabled", "True")

        # Load jdbc driver jar from local path location
        # .config("spark.jars", str(Path(os.environ["PG_JDBC_DRIVER_PATH"])))

        # or download the jdbc driver directly from Maven repository
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.4")
        .appName(app_name)
        .getOrCreate()
    )

def read_query(spark: SparkSession, sql: str, **extra_options) -> DataFrame:
    """Run a SQL query against Postgres and return a Spark DataFrame."""
    return (
        spark.read.format("jdbc")
        .options(**JDBC_OPTIONS)
        .option("query", sql)
        .options(**extra_options)
        .load()
    )

def read_table(spark: SparkSession, table: str, **extra_options) -> DataFrame:
    """Read a whole table (Spark pushes down filters where it can)."""
    return (
        spark.read.format("jdbc")
        .options(**JDBC_OPTIONS)
        .option("dbtable", table)
        .options(**extra_options)
        .load()
    )

def write_dataframe(df: DataFrame, table: str, mode: str = "overwrite", **extra_options) -> None:
    """Write a PySpark DataFrame to a Postgres table."""
    (
        df
        .write
        .format("jdbc")
        .options(**JDBC_OPTIONS)
        .option("dbtable", table)
        .options(**extra_options)
        .mode(mode)
        .save()
    )