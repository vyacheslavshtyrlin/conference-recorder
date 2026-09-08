#!/bin/sh
set -eu

storage_root="${RECORDING_STORAGE_ROOT:-/var/lib/conference-recording}"
mkdir -p "$storage_root"
mkdir -p "$storage_root/composition"
chown node:node "$storage_root" "$storage_root/composition"

exec su-exec node "$@"
