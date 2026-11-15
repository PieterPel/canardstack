INSTALL ducklake;
LOAD ducklake;
INSTALL postgres;
LOAD postgres;

ATTACH 'ducklake:postgres:dbname=ducklake host=postgres user=postgres password=bench' AS canardlake (
  DATA_PATH '/var/lib/ducklake/data/',
  AUTOMATIC_MIGRATION true
);
USE canardlake;
