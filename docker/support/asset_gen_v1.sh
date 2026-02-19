#!/bin/bash

# asset_gen_v1 is a superset of image_gen_v2:
# keep all image_gen_v2 provisioning behavior and override SERVER_TYPE by default.
export SERVER_TYPE="${SERVER_TYPE:-asset_gen_v1}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${SCRIPT_DIR}/image_gen_v2.sh"
