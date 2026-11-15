INSTALL ducklake;
LOAD ducklake;
INSTALL postgres;
LOAD postgres;

-- DuckLake catalog in Postgres. Credentials must match the postgres service in
-- compose.postgres.yaml (local development defaults).
ATTACH 'ducklake:postgres:dbname=ducklake host=postgres user=postgres password=ducklake-dev-password' AS canardlake (
  DATA_PATH '/var/lib/ducklake/data/',
  AUTOMATIC_MIGRATION true
);
USE canardlake;
