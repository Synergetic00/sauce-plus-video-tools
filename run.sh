#!/bin/bash

mkdir -p credentials
mkdir -p downloaded
mkdir -p encoded

CURRENT_DIR=$(cygpath -w "$(pwd)" | sed 's/\\/\//g')

docker run --rm \
  -p 5000:5000 \
  --env-file .env \
  -v "${CURRENT_DIR}/downloaded:/app/downloaded" \
  -v "${CURRENT_DIR}/encoded:/app/encoded" \
  -v "${CURRENT_DIR}/credentials:/app/credentials:ro" \
  youtube-mirror
