#!/bin/bash

FILENAME="./arrays_table.csv"
URL="https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-LX0117EN-SkillsNetwork/labs/M3/L2/arrays_table.csv"

if [ ! -f $FILENAME ]; then
    wget $URL
fi

arr1="$(cut -d"," -f1 $FILENAME)"
arr2="$(cut -d"," -f2 $FILENAME)"
arr3="$(cut -d"," -f3 $FILENAME)"