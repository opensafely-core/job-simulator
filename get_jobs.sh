#!/bin/bash
set -euo pipefail

tmp_file='jobs.tmp.duckdb'
out_file='jobs.duckdb'
rm -f "$tmp_file"

ssh dokku4.ebmdatalab.net sqlite3 < query_jobs.sql | \
  uvx --from duckdb-cli duckdb "$tmp_file" \
    -c "CREATE TABLE jobs AS SELECT * FROM read_json('/dev/stdin');"

mv "$tmp_file" "$out_file"
