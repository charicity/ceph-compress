#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS_DIR="$SCRIPT_DIR/logs"
if [ -d "$LOGS_DIR" ]; then
    echo "Cleaning up logs in $LOGS_DIR"
    rm -rf "$LOGS_DIR"/*
    echo "Logs cleaned."
else
    echo "Logs directory $LOGS_DIR does not exist."
fi