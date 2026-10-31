INSTALL ducklake;
LOAD ducklake;
INSTALL sqlite;
LOAD sqlite;

ATTACH 'ducklake:sqlite:/var/lib/ducklake/meta/catalog.sqlite' AS canardlake (
  DATA_PATH '/var/lib/ducklake/data/',
  AUTOMATIC_MIGRATION true
);
USE canardlake;
