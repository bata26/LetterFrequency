#!/bin/bash
listDimens=(50MB 500MB 1GB 3GB)
for dim in ${listDimens[@]}; do
#	hadoop fs -put FR_${dim}.txt /user/hadoop/FR_${dim}.txt
#	hadoop fs -put EN_${dim}.txt /user/hadoop/EN_${dim}.txt
	hadoop fs -put IT_${dim}.txt /user/hadoop/IT_${dim}.txt
done
