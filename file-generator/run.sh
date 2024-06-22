#!/bin/bash
listDimens=(50MB 250MB 500MB 750MB)
for dim in ${listDimens[@]}; do
	hadoop fs -put FR_${dim}.txt /user/hadoop/FR_${dim}.txt
	hadoop fs -put EN_${dim}.txt /user/hadoop/EN_${dim}.txt
	hadoop fs -put IT_${dim}.txt /user/hadoop/IT_${dim}.txt
done
