#hadoop fs -put 50_MB.txt /user/hadoop/50_MB.txt
#hadoop fs -put 250_MB.txt /user/hadoop/250_MB.txt
#hadoop fs -put 500_MB.txt /user/hadoop/500_MB.txt
#hadoop fs -put 1_GB.txt /user/hadoop/1_GB.txt
#hadoop fs -put 2_GB.txt /user/hadoop/2_GB.txt
#hadoop fs -put 3_GB.txt /user/hadoop/3_GB.txt
#hadoop fs -put 4_GB.txt /user/hadoop/4_GB.txt
#hadoop fs -put 5_GB.txt /user/hadoop/5_GB.txt

mvn -f lettercount-naive clean package
#sudo mvn -f lettercount-mid clean package
mvn -f lettercount-opt clean package
mvn -f lettercount-opt-combiner clean package

# ↑↑↑ Uncomment only on the first execution ↑↑↑

# IT_50MB.txt IT_500MB.txt IT_1GB.txt IT_3GB.txt IT_5GB.txt
# EN_50MB.txt EN_500MB.txt EN_1GB.txt EN_3GB.txt EN_5GB.txt
# FR_50MB.txt FR_500MB.txt FR_1GB.txt FR_3GB.txt FR_5GB.txt

files=(IT_50MB.txt IT_500MB.txt IT_1GB.txt IT_3GB.txt IT_5GB.txt)
versions=(naive opt opt-combiner)
reducers=(1 7 13 20 26)

for file in ${files[@]}; 
do
for ver in ${versions[@]};
    do 
    for num in ${reducers[@]}; 
        do
        echo ---------------------------------------------------------------------
        echo starting execution version: ${ver}, file: ${file}, num reducer: ${num}
        echo ---------------------------------------------------------------------
        hadoop jar ./lettercount-${ver}/target/lettercount-${ver}-1.0-SNAPSHOT.jar it.unipi.hadoop.LetterCount ${file} results/${ver}/${file}/${num} ${num};
        echo waiting for killing container....
        sleep 300; # Wait 5 min. to let all containers finish (60sec x 5)
        done; 
    done;
done