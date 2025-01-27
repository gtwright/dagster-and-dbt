from io import BytesIO

import pandas as pd
import requests
from dagster import AssetExecutionContext, MaterializeResult, MetadataValue, asset
from dagster_duckdb import DuckDBResource
from dagster_snowflake import SnowflakeIOManager, SnowflakeResource
from smart_open import open
import os

from ..partitions import monthly_partition
from ..resources import smart_open_config
from . import constants


@asset(
    group_name="raw_files",
    compute_kind="Python",
)
def taxi_zones_file() -> MaterializeResult:
    """The raw CSV file for the taxi zones dataset. Sourced from the NYC Open Data portal."""
    # raw_taxi_zones = requests.get(
    #     "https://data.cityofnewyork.us/api/views/755u-8jsi/rows.csv?accessType=DOWNLOAD"
    # )

    # with open(
    #     constants.TAXI_ZONES_FILE_PATH, "wb", transport_params=smart_open_config
    # ) as output_file:
    #     output_file.write(raw_taxi_zones.content)

    with open("data/raw/taxi_zones.csv", 'rb') as file:
        raw_zones = file.read()

    num_rows = len(pd.read_csv(BytesIO(raw_zones)))
    return MaterializeResult(metadata={"Number of records": MetadataValue.int(num_rows)})


@asset(
    deps=["taxi_zones_file"],
    group_name="ingested",
    compute_kind="Snowflake",
    io_manager_key="snowflake_io_manager"
)
def taxi_zones(context: AssetExecutionContext) -> pd.DataFrame:
    """The raw taxi zones dataset, loaded into a DuckDB database."""
    file_path = constants.TAXI_ZONES_FILE_PATH

    context.log.info(f"Loading zones data from: {file_path}")
    df = pd.read_csv(file_path)
    return df


@asset(
    partitions_def=monthly_partition,
    group_name="raw_files",
    compute_kind="Snowflake",
)
def taxi_trips_file(context: AssetExecutionContext) -> MaterializeResult:
    """The raw parquet files for the taxi trips dataset. Sourced from the NYC Open Data portal."""
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]

    raw_trips = requests.get(
        f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month_to_fetch}.parquet"
    )

    with open(
        constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch),
        "wb",
        transport_params=smart_open_config,
    ) as output_file:
        output_file.write(raw_trips.content)

    num_rows = len(pd.read_parquet(BytesIO(raw_trips.content)))
    return MaterializeResult(metadata={"Number of records": MetadataValue.int(num_rows)})


@asset(
    deps=["taxi_trips_file"],
    partitions_def=monthly_partition,
    group_name="ingested",
    compute_kind="Snowflake",
    io_manager_key="snowflake_io_manager",
    metadata={
        "partition_expr": "partition_date"
    }
)
def taxi_trips(context: AssetExecutionContext) -> pd.DataFrame:
    """The raw taxi trips dataset, loaded into a DuckDB database, partitioned by month."""
    partition_date_str = context.partition_key
    month_to_fetch = partition_date_str[:-3]
    
    file_path = constants.TAXI_TRIPS_TEMPLATE_FILE_PATH.format(month_to_fetch)
    context.log.info(f"Loading data from: {file_path}")
    
    # Read the parquet file into a pandas DataFrame
    df = pd.read_parquet(file_path)
    
    # Add partition column
    df['partition_date'] = month_to_fetch
    
    # Return the DataFrame - the Snowflake IO Manager will handle loading it into Snowflake
    return df
