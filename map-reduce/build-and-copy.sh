#!/bin/bash

set -e

PROJECT_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "Project root: $PROJECT_ROOT"

cd "$PROJECT_ROOT"

echo "Building project with Maven..."
mvn clean install

JAR_FILE=$(find "$PROJECT_ROOT/target" -maxdepth 1 -name "*.jar" | head -n 1)

if [ -z "$JAR_FILE" ]; then
  echo "No JAR file found in target directory!"
  exit 1
fi

DEST_DIR="$PROJECT_ROOT/src/main/resources"

mkdir -p "$DEST_DIR"

echo "Copying JAR to $DEST_DIR..."
cp -f "$JAR_FILE" "$DEST_DIR/"

echo "Build and copy completed successfully."