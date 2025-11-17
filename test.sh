#!/usr/bin/env bash
set -e

# Script expects TARGET to be passed from pipeline
if [ -z "$TARGET" ]; then
  echo "TARGET not set. Pass TARGET=<dev|test|prod> from Azure DevOps."
  exit 1
fi

echo "Running bundle for target: $TARGET"

databricks bundle run pipeline -t "$TARGET"



#!/usr/bin/env bash
set -e

########################################
# User-configured section
########################################

BRONZE_CATALOG="bronze_catalog"
BRONZE_SCHEMA="bronze_schema"

TABLE_LIST=("table1" "table2" "table3")

########################################
# Mandatory TARGET check
########################################

if [ -z "$TARGET" ]; then
  echo "TARGET not set. Pass TARGET=<dev|test|prod> from Azure DevOps."
  exit 1
fi

########################################
# Build full-refresh string
########################################

FULL_REFRESH_STRING=""
for tbl in "${TABLE_LIST[@]}"; do
  FULL_REFRESH_STRING+="${BRONZE_CATALOG}.${BRONZE_SCHEMA}.${tbl},"
done

# Remove trailing comma
FULL_REFRESH_STRING="${FULL_REFRESH_STRING%,}"

echo "Running pipeline for target: $TARGET"
echo "Full refresh tables: $FULL_REFRESH_STRING"

########################################
# Execute
########################################

databricks bundle run pipeline -t "$TARGET" \
  --full-refresh "$FULL_REFRESH_STRING"

