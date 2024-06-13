sudo mvn clean package
#hadoop fs -put input.txt /user/hadoop/input.txt
hadoop fs -test -e output
if [ $(echo $?) = '0' ]; then
	hadoop fs -rm -r output
fi
hadoop jar ./target/lettercount-basic-1.0-SNAPSHOT.jar it.unipi.hadoop.LetterCountMain input.txt output 4