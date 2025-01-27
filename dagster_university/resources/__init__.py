import os

import boto3
from dagster import EnvVar
from dagster_duckdb import DuckDBResource
from dagster_snowflake import SnowflakeResource
from dagster_snowflake_pandas import SnowflakePandasIOManager
from dagster_dbt import DbtCliResource

from ..project import dbt_project

dbt_resource = DbtCliResource(
    project_dir=dbt_project,
)

database_resource = DuckDBResource(
    database=EnvVar("DUCKDB_DATABASE"),
)

snowflake_pandas_io_manager = SnowflakePandasIOManager(
    database=EnvVar("SNOWFLAKE_DATABASE"),
    schema=EnvVar("SNOWFLAKE_SCHEMA"),
    role=EnvVar("SNOWFLAKE_ROLE"),
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
)

snowflake_resource = SnowflakeResource(
    account=EnvVar("SNOWFLAKE_ACCOUNT"),
    user=EnvVar("SNOWFLAKE_USER"),
    password=EnvVar("SNOWFLAKE_PASSWORD"),
    database=EnvVar("SNOWFLAKE_DATABASE"),
    schema=EnvVar("SNOWFLAKE_SCHEMA"),
    role=EnvVar("SNOWFLAKE_ROLE"),
    warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
)

if os.getenv("DAGSTER_ENVIRONMENT") == "prod":
    session = boto3.Session(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION"),
    )
    smart_open_config = {"client": session.client("s3")}
else:
    smart_open_config = {}
