#!/bin/sh
set -eu
wait-for-services.sh
pytest /app/tests