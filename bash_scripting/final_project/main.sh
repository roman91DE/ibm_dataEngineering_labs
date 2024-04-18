#!/bin/bash

set -euo pipefail


TIMESTAMP=$(date "+%Y-%m-%d_%H-%M-%S")
URL="wttr.in/"
LOCATION="casablanca"
JUST_TXT="?T" # this forces ASCII plaintext as response instead of ANSI


PROJECT_DIR="./bash_scripting/final_project"
LOG_FILE="$PROJECT_DIR/rx_poc.log"
RAW_RESP_FILE="$PROJECT_DIR/${LOCATION}-raw-$TIMESTAMP.txt"


if [ ! -e "$LOG_FILE" ]; then
    echo -e "year\tmonth\tday\thour\tobs_temp\tfc_temp" > $LOG_FILE
fi

curl "$URL$LOCATION$JUST_TXT" > "$RAW_RESP_FILE" 

OBS_LINE=3
OBS_VAL=$(grep -oE '[0-9]+ °C' "$RAW_RESP_FILE" | cut -d " " -f1 | sed -n "${OBS_LINE}p")

echo $OBS_VAL

FC_LINE=7
FC_VAL=$(grep -oE '[0-9]+ °C' "$RAW_RESP_FILE" | cut -d " " -f1 | sed -n "${FC_LINE}p")


echo $FC_VAL