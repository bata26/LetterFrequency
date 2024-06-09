# letterFrequency
Project for Cloud Computing Course at unipi


# Credentials

| name  | IP         | user | password | pwd hadoop |
| ----- | ---------- | ---- | -------- | ---------- |
| bata  | 10.1.1.82  | root | ubuntu   | ubuntu     |
| edo   | 10.1.1.143 | root | ubuntu   | hadoop     |
| kevin | 10.1.1.82  | root | ubuntu   | ijnokmpl   |

# To execute

Compile
```shell
mvn clean package
```
Move the input file in hdfs

```shell
hadoop fs -put test.txt /user/hadoop/test.txt
```

Run the application

```shell
hadoop jar ./target/lettercount-1.0-SNAPSHOT.jar it.unipi.hadoop.LetterCount test.txt output 4
```

There could be problems with file, in that case access the dashboard at *10.1.1.82:9870/explorer.html* and check if the DHFS is setted up correctly
