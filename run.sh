#!/bin/bash

mvn -f lettercount-naive clean package
#sudo mvn -f lettercount-mid clean package
mvn -f lettercount-opt clean package
mvn -f lettercount-opt-combiner clean package

files=(IT_1GB.txt)
versions=(naive)
reducers=(26)

for file in ${files[@]}; do
    for ver in ${versions[@]}; do
        for num in ${reducers[@]}; do
            log_file="logs/log_${ver}_${file}_${num}.txt"
            mkdir -p logs # Ensure the logs directory exists
		echo "---------------------------------------------------------------------"
                echo "starting execution version: ${ver}, file: ${file}, num reducer: ${num}"
                echo "---------------------------------------------------------------------"
           {
                hadoop jar ./lettercount-${ver}/target/lettercount-${ver}-1.0-SNAPSHOT.jar it.unipi.hadoop.LetterCount ${file} results/${ver}/${file}/${num} ${num}
                echo "waiting for killing container...."
                sleep 150 # Wait 5 min. to let all containers finish (60sec x 5)
            } >> ${log_file} 2>&1
        done
    done
done
