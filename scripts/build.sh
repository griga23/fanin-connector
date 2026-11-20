#!/usr/bin/env bash
# Build, test and package the plugin archive. Output: target/components/packages/*.zip
set -euo pipefail
cd "$(dirname "$0")/.."
mvn -B clean verify "$@"
ZIP=$(ls target/components/packages/*.zip)
echo "Archive: $ZIP ($(du -h "$ZIP" | cut -f1))"
unzip -l "$ZIP"
