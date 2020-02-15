# spark-rqg
This project provides a random query generator to capture the correctness bugs by comparing results between different versions of Apache Spark.

## Download
```
https://github.com/LinhongLiu/spark-rqg.git
```

## Build
```
sbt package
```

## Prerequisities
```
wget https://www.apache.org/dyn/closer.lua/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz
tar -xzvf spark-3.0.0-preview2-bin-hadoop2.7.tgz
cd spark-3.0.0-preview2-bin-hadoop2.7
./sbin/start-thriftserver.sh
```

## Generate Data
```
sbt "runMain com.baidu.spark.rqg.DataGenerator --randomizationSeed=10 --minRowCount=100 --maxRowCount=200"
```

## Run Generated Queries
```
sbt "runMain com.baidu.spark.rqg.comparison.DiscrepancySearcher --queryCount=10"
```
