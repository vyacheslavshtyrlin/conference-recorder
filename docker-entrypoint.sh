#!/bin/sh
set -eu

storage_root="${RECORDING_STORAGE_ROOT:-/var/lib/conference-recording}"
mkdir -p "$storage_root"
chown -R pwuser:pwuser "$storage_root"

exec setpriv --reuid=pwuser --regid=pwuser --init-groups -- "$@"
