#!/bin/sh
set -eu

: "${FUNCTIONS_CUSTOMHANDLER_PORT:?Azure Functions must assign the handler port}"

if [ -z "${CANARDSTACK_DATA_DIR:-}" ]; then
  CANARDSTACK_DATA_DIR="$(mktemp -d "${TMPDIR:-/tmp}/canardstack.XXXXXX")"
  export CANARDSTACK_DATA_DIR
fi

exec ./canardstack serve --listen "127.0.0.1:${FUNCTIONS_CUSTOMHANDLER_PORT}"
