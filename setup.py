from setuptools import find_packages, setup

setup(
    name="dagster_university",
    packages=find_packages(exclude=["dagster_university_tests"]),
    install_requires=[
        "dagster==1.9.*",
        "dagster-cloud",
        "dagster-duckdb",
        "dagster-snowflake",
        "dagster-snowflake-pandas",
        "dagster-dbt",
        "dbt-duckdb",
        "dbt-snowflake",
        "geopandas",
        "kaleido",
        "pandas[parquet]",
        "plotly",
        "shapely",
        "smart_open[s3]",
        "s3fs",
        "smart_open",
        "boto3",
        "pyarrow",
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
