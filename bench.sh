#!/bin/bash

set -e
HOST=localhost
PORT=6380
N=1000000
C=50

redis-benchmark -h $HOST -p $PORT -t set,get -n $N -c $C -q | tee base.txt 