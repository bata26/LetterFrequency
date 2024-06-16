#hadoop fs -put 50_MB.txt /user/hadoop/50_MB.txt
#hadoop fs -put 250_MB.txt /user/hadoop/250_MB.txt
#hadoop fs -put 500_MB.txt /user/hadoop/500_MB.txt
#hadoop fs -put 1_GB.txt /user/hadoop/1_GB.txt
#hadoop fs -put 2_GB.txt /user/hadoop/2_GB.txt
#hadoop fs -put 3_GB.txt /user/hadoop/3_GB.txt
#hadoop fs -put 4_GB.txt /user/hadoop/4_GB.txt
#hadoop fs -put 5_GB.txt /user/hadoop/5_GB.txt

sudo mvn -f lettercount-naive clean package
sudo mvn -f lettercount-mid clean package
sudo mvn -f lettercount-opt clean package

# ↑↑↑ Uncomment only on the first execution ↑↑↑

files=(moby_EN.txt moby_FR.txt moby_IT.txt)
versions=(opt)
reducers=(1 7 13 20 26)

for file in ${files[@]}; 
do
for ver in ${versions[@]};
    do 
    for num in ${reducers[@]}; 
        do
        hadoop jar ./lettercount-${ver}/target/lettercount-${ver}-1.0-SNAPSHOT.jar it.unipi.hadoop.LetterCount ${file} results/${ver}/${file}/${num} ${num};
        sleep 300; # Wait 5 min. to let all containers finish (60sec x 5)
        done; 
    done;
done