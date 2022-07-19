#!/usr/bin/env bash

# Usage  ./ploter pid repetitions delay

for tbl in $(cat topics.txt)
do
confluent kafka topic create $tbl --partitions 1
done