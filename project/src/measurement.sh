#!/bin/sh
N=10

if [ ! -e ../data/time_logs ]; then
    mkdir ../data/time_logs
fi

for i in `seq 1 $N`
do
    echo $i 回目の計測
    python main.py > ../data/time_logs/time$i.log
done
