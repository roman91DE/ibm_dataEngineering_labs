#!/bin/bash

echo "Are you stupid? [Yes/No]"
read -r  ANSWER


if [ "$ANSWER" == "Yes" ]
then
    echo "Dumbo"
else 
    if [ "$ANSWER" == "No" ]
    then 
        echo "IQ 2000"
    else
        echo "Do it again!"
    fi
fi

