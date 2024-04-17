#!/bin/bash


echo First int:
read -r A

echo Second int:
read -r B

PROD=$((A * B))
SUM=$((A + B))

echo -e "Product = $PROD\nSum = $SUM"

if [ $SUM -gt $PROD ] 
then
    echo "sum is larger than product"
else
    if [ $SUM -lt $PROD ] 
    then
        echo "sum is smaller than product"
    else
        echo "both are equal"
    fi
fi        
