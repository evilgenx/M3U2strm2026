#!/bin/bash
set -e

# -------------------------------------------------------------------
# M3U2strm2026 Docker Entrypoint
# -------------------------------------------------------------------

CONFIG_FILE="${CONFIG_FILE:-/app/config.json}"
DATA_DIR="${DATA_DIR:-/app/data}"
LOG_FILE="${LOG_FILE:-/app/data/m3u2strm.log}"
CACHE_FILE="${CACHE_FILE:-/app/data/caches.db}"

echo "=== M3U2strm2026 ==="
echo "Port: ${PORT:-8080}"
echo "Config: $CONFIG_FILE"
echo "Data dir: $DATA_DIR"

# Ensure data directory exists
mkdir -p "$DATA_DIR"

# If config.json doesn't exist in the expected location, create from example
if [ ! -f "$CONFIG_FILE" ]; then
    echo "WARNING: $CONFIG_FILE not found. Copying config.json.example as a template."
    if [ -f /app/config.json.example ]; then
        cp /app/config.json.example "$CONFIG_FILE"
        echo "Please edit $CONFIG_FILE and restart the container."
    fi
fi

# Export vars for the Flask app
export CONFIG_FILE
export DATA_DIR
export LOG_FILE
export CACHE_FILE

echo "Starting Gunicorn on 0.0.0.0:${PORT:-8080}..."
exec gunicorn --bind "0.0.0.0:${PORT:-8080}" \
    --workers 2 \
    --threads 4 \
    --worker-class gthread \
    --timeout 120 \
    --access-logfile - \
    --error-logfile - \
    --capture-output \
    "web.app:app"