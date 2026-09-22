#!/usr/bin/env bash
# Splits a large input file into smaller files with PySpark (Phase 2).
set -euo pipefail

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [pyspark-split] execution_id=${EXECUTION_ID} | $*"; }

log "input=${INPUT_FILE} type=${FILE_TYPE} output=${SPLIT_OUTPUT_DIR}"

python /opt/lumi/pyspark/jobs/split_file.py \
    --input-file "${INPUT_FILE}" \
    --file-type "${FILE_TYPE}" \
    --execution-id "${EXECUTION_ID}" \
    --output-dir "${SPLIT_OUTPUT_DIR}" \
    --records-per-file "${LUMI_RECORDS_PER_SPLIT_FILE:-50}"

log "done"
