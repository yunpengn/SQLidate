#!/usr/bin/env bash

load_data() {
  cd "$(dirname "$0")"
  cd ..
  ./gradlew shadowJar
  java -jar ./build/libs/SQLidate-1.0-SNAPSHOT-all.jar load 13
}

check_query() {
  cd "$(dirname "$0")"
  cd ..
  ./gradlew shadowJar
  java -jar ./build/libs/SQLidate-1.0-SNAPSHOT-all.jar check /Users/yunpeng/Projects/calcite/core/output_0.txt
}

generate() {
  cd "$(dirname "$0")"
  psql random -f create_db.sql
  psql random -f insert_data_small.sql
}

generateBig() {
  cd "$(dirname "$0")"
  psql random -f create_db_big.sql

  cd tables/
  psql random -f a.sql
  psql random -f b.sql
  psql random -f c.sql
  psql random -f d.sql
  psql random -f e.sql
  psql random -f f.sql
}

if [[ "$1" == "load" ]]; then
  load_data
elif [[ "$1" == "generate" ]]; then
  generate
elif [[ "$1" == "generateBig" ]]; then
  generateBig
elif [[ "$1" == "check" ]]; then
  check_query
else
  echo "Unknown command. Please use \"./run.sh [load/check/generate]\" for the script."
fi
