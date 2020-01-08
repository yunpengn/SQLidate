#!/usr/bin/env bash

load_data() {
  cd "$(dirname "$0")"
  ./gradlew shadowJar
  java -jar ./build/libs/SQLidate-1.0-SNAPSHOT-all.jar load 20
}

check_query() {
  cd "$(dirname "$0")"
  ./gradlew shadowJar
  java -jar ./build/libs/SQLidate-1.0-SNAPSHOT-all.jar check /Users/yunpeng/Projects/calcite/core/output.txt
}

if [[ "$1" == "load" ]]; then
  load_data
elif [[ "$1" == "check" ]]; then
  check_query
else
  echo "Unknown command. Please use \"./run.sh [load/check]\" for the script."
fi
