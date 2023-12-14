#!/usr/bin/env bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
for file in "${CUR_DIR}/*.test.yaml"; do
  po-test  $file
done
