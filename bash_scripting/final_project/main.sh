#!/bin/bash

set -euo pipefail


TIMESTAMP=$(date "+%Y-%m-%d_%H-%M-%S")
URL="wttr.in/"
LOCATION="casablanca"


PROJECT_DIR="./bash_scripting/final_project"
LOG_FILE="$PROJECT_DIR/rx_poc.log"
RAW_RESP_FILE="$PROJECT_DIR/LOCATION-raw-$TIMESTAMP.txt"


if [ ! -e "$LOG_FILE" ]; then
    echo -e "year\tmonth\tday\thour\tobs_temp\tfc_temp" > $LOG_FILE
fi

curl "$URL$LOCATION" > "$RAW_RESP_FILE"
