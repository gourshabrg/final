#!/usr/bin/env bash
# Runs the Beam job on the original file, or on the split files when PySpark split it.
set -euo pipefail

log() { echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) [beam] execution_id=${EXECUTION_ID} | $*"; }

BEAM_JAR=/opt/lumi/beam/beam-ingestion-1.0.0-SNAPSHOT.jar
if [ ! -f "${BEAM_JAR}" ]; then
    log "ERROR: ${BEAM_JAR} not found. Build it with: cd beam-ingestion && ./mvnw package"
    exit 1
fi

if [ "${REQUIRES_SPLIT}" = "true" ]; then
    EXTENSION=$(echo "${FILE_TYPE}" | tr '[:upper:]' '[:lower:]')
    SPLIT_COUNT=$(find "${SPLIT_OUTPUT_DIR}" -maxdepth 1 -type f -name "*.${EXTENSION}" | wc -l)
    if [ "${SPLIT_COUNT}" -eq 0 ]; then
        log "ERROR: no .${EXTENSION} files in ${SPLIT_OUTPUT_DIR}"
        exit 1
    fi
    BEAM_INPUT="${SPLIT_OUTPUT_DIR}/*.${EXTENSION}"
    log "reading ${SPLIT_COUNT} split file(s): ${BEAM_INPUT}"
else
    BEAM_INPUT="${INPUT_FILE}"
    log "reading original file: ${BEAM_INPUT}"
fi

java -jar "${BEAM_JAR}" \
    --inputFile="${BEAM_INPUT}" \
    --fileType="${FILE_TYPE}" \
    --executionId="${EXECUTION_ID}" \
    --controlFile="${CONTROL_FILE}" \
    --errorOutput="${ERROR_OUTPUT}" \
    --encryptionKey="${LUMI_ENCRYPTION_KEY}" \
    --jdbcUrl="${LUMI_WAREHOUSE_JDBC_URL}" \
    --jdbcUsername="${LUMI_WAREHOUSE_USERNAME}" \
    --jdbcPassword="${LUMI_WAREHOUSE_PASSWORD}"

log "finished; errors (if any) in ${ERROR_OUTPUT}.txt"
