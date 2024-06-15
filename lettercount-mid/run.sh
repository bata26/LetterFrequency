sudo mvn clean package
#hadoop fs -put input.txt /user/hadoop/input.txt
hadoop fs -test -e output
if [ $(echo $?) = '0' ]; then
	hadoop fs -rm -r output
fi
	hadoop jar ./lettercount-mid/target/lettercount-basic-1.0-SNAPSHOT.jar it.unipi.hadoop.LetterCountMain en-1_GB output-mid-50MB-4 4

	hadoop jar ./lettercount-opt/target/lettercount-basic-1.0-SNAPSHOT.jar it.unipi.hadoop.LetterCountMain en-1_GB output 4
