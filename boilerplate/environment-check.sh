#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run this script with sudo so namespace and chroot checks are meaningful."
  exit 1
fi

echo "[ok] running as root"
command -v gcc >/dev/null && echo "[ok] gcc present"
command -v make >/dev/null && echo "[ok] make present"

if [[ -d "/lib/modules/$(uname -r)/build" ]]; then
  echo "[ok] kernel headers found for $(uname -r)"
else
  echo "[warn] kernel headers missing for $(uname -r)"
fi

if [[ -w /tmp ]]; then
  echo "[ok] /tmp writable"
else
  echo "[warn] /tmp not writable"
fi

echo "Environment preflight complete."
