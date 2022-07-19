#!/usr/bin/env bash

# Usage  ./ploter pid repetitions delay

for tbl in $(confluent kafka topic list | tail -n +3)
do
confluent kafka topic delete $tbl
done