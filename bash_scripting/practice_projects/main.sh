#!/bin/bash

set -euo pipefail


TIMESTAMP=$(date "+%Y-%m-%d_%H-%M-%S")
URL="wttr.in/"
LOCATION="casablanca"
JUST_TXT="?T" # this forces ASCII plaintext as response instead of ANSI


PROJECT_DIR="./bash_scripting/practice_projects"
LOG_FILE="$PROJECT_DIR/rx_poc.tsv"
RAW_RESP_FILE="$PROJECT_DIR/${LOCATION}-raw-$TIMESTAMP.txt"


if [ ! -e "$LOG_FILE" ]; then
    echo -e "year\tmonth\tday\thour\tobs_temp\tfc_temp" > $LOG_FILE
fi

curl "$URL$LOCATION$JUST_TXT" > "$RAW_RESP_FILE" 

OBS_LINE=3
OBS_VAL=$(grep -oE '[0-9]+ °C' "$RAW_RESP_FILE" | cut -d " " -f1 | sed -n "${OBS_LINE}p")

FC_LINE=7
FC_VAL=$(grep -oE '[0-9]+ °C' "$RAW_RESP_FILE" | cut -d " " -f1 | sed -n "${FC_LINE}p")


YEAR=$(date "+%Y")
MONTH=$(date "+%m")
DAY=$(date "+%d")
HOUR=$(date "+%H")


echo -e "$YEAR\t$MONTH\t$DAY\t$HOUR\t$OBS_VAL\t$FC_VAL" >> "$LOG_FILE"
echo "Successfully logged Temperatures"