#!bin/bash

for REDUCER in 2 4 8 16 32 64
do
    echo "Tiempo para $REDUCER reducers"
    time $1 $2 $3 $REDUCER
done