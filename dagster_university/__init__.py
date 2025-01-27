from dagster import Definitions, load_assets_from_modules, EnvVar
from dagster_snowflake_pandas import SnowflakePandasIOManager

from .assets import metrics, requests, trips, dbt
from .jobs import adhoc_request_job, trip_update_job, weekly_update_job
from .resources import database_resource, dbt_resource, snowflake_pandas_io_manager, snowflake_resource
from .schedules import trip_update_schedule, weekly_update_schedule
from .sensors import adhoc_request_sensor

trip_assets = load_assets_from_modules([trips])
metric_assets = load_assets_from_modules(
    modules=[metrics],
    group_name="metrics",
)
requests_assets = load_assets_from_modules(
    modules=[requests],
    group_name="requests",
)
dbt_analytics_assets = load_assets_from_modules(modules=[dbt])

all_jobs = [trip_update_job, weekly_update_job, adhoc_request_job]
all_schedules = [trip_update_schedule, weekly_update_schedule]
all_sensors = [adhoc_request_sensor]

defs = Definitions(
    assets=trip_assets + metric_assets + requests_assets + dbt_analytics_assets,
    resources={
        "dbt": dbt_resource,
        "snowflakeDB": snowflake_resource,
        "database": database_resource,   
        "snowflake": snowflake_resource,
        "snowflake_io_manager": snowflake_pandas_io_manager
    },
    jobs=all_jobs,
    schedules=all_schedules,
    sensors=all_sensors,
)
