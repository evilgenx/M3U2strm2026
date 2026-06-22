#!/bin/bash
# -------------------------------------------------------------------
# M3U2strm2026 Docker Entrypoint
#
# All output (logs, cache, STRM files) lives under /app/output/.
# This script ensures those directories exist before Gunicorn starts.
# -------------------------------------------------------------------

CONFIG_FILE="${CONFIG_FILE:-/app/config.json}"
OUTPUT_DIR="${OUTPUT_DIR:-/app/output}"

echo "=== M3U2strm2026 ==="
echo "Running as: $(id -u):$(id -g)"
echo "Port: ${PORT:-8080}"
echo "Config: $CONFIG_FILE"
echo "Output dir: $OUTPUT_DIR"

# -------------------------------------------------------------------
# Create output directory tree
# -------------------------------------------------------------------
mkdir -p "$OUTPUT_DIR/logs" "$OUTPUT_DIR/strm"
echo "Output directories ready: $OUTPUT_DIR"

# -------------------------------------------------------------------
# If config.json doesn't exist, create from the example template.
# -------------------------------------------------------------------
if [ ! -f "$CONFIG_FILE" ]; then
    echo "WARNING: $CONFIG_FILE not found. Copying config.json.example as a template."
    if [ -f /app/config.json.example ]; then
        cp /app/config.json.example "$CONFIG_FILE"
        echo "Created $CONFIG_FILE from example template."
    fi
fi

# Export vars for the Flask app
export CONFIG_FILE
export OUTPUT_DIR

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