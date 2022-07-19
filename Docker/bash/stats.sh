#!/usr/bin/env bash

# Usage  ./ploter pid repetitions delay

rm $1.tsv
#printf "%%mem\trss\tvsz\n" >> $1.tsv
for i in $(seq $2); do 
    #ps -p 63531 -o %mem,rss,vsz | tail -1 
    #docker stats --no-stream --format "{{ json . }}" >> $1.tsv
    docker stats --no-stream  --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.MemPerc}}" | tail -n +2 | grep -v my-schema-project | sed  s/$/\\t$i/>> $1.tsv
    sleep $3;
done