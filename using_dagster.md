# Installing dagster
#### https://dagster.io/docs/getting-started/installation
pip install dagster dagster-webserver dagster-dg-cli dagster-postgres

# Configure how to run dagster locally with dagster.yaml file
#### https://dagster.io/docs/deployment/oss/deployment-options/running-dagster-locally
set DAGSTER_HOME=some_path

Create dagster.yaml file and save it in your DAGSTER_HOME location

Then add the following to it if you don't want to share telemetry data:
telemetry:
  enabled: false

#### To avoid warning when launching dagster webserver on Windows
set PYTHONLEGACYWINDOWSSTDIO=1

# Running dagster as a service
#### https://dagster.io/docs/deployment/oss/deployment-options/deploying-dagster-as-a-service

# Store dagster runs, events, and schedules in PostgreSQL
#### https://dagster.io/docs/integrations/libraries/postgres/dagster-postgres

Save the following in the dagster.yaml file:
storage:
  postgres:
    postgres_db:
      username: my_username
      password: my_password
      hostname: my_hostname
      db_name: my_database
      port: 5432

# Using Postgres as a dagster resource
https://dagster.io/docs/guides/build/external-resources/defining-resources

Example Postgres Pydantic configurable resource class:

```{python}
class PostgresResource(dg.ConfigurableResource):
    """Writes Polars DataFrames to Postgres using the ADBC engine."""

    host: str
    port: str
    user: str
    password: str
    database: str

    @property
    def connection_uri(self) -> str:
        return (
            f"postgresql://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    def write_table(self, df: pl.DataFrame, table_name: str) -> None:
        df.write_database(
            table_name=table_name,
            connection=self.connection_uri,
            engine="adbc",
            if_table_exists="replace",
        )
```

