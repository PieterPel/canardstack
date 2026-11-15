INSTALL quack;
LOAD quack;
INSTALL ducklake;
LOAD ducklake;

CREATE OR REPLACE SECRET canardstack_quack_tls (
  TYPE HTTP,
  SCOPE 'https://quack-tls:9494',
  VERIFY_SSL 0
);
CREATE OR REPLACE SECRET canardstack_ducklake_quack (
  TYPE quack,
  SCOPE 'quack:quack-tls:9494',
  TOKEN '__QUACK_TOKEN__'
);

ATTACH 'ducklake:quack:quack-tls:9494' AS canardlake (
  DATA_PATH '/var/lib/ducklake/data/',
  AUTOMATIC_MIGRATION true
);
USE canardlake;
