# spark-rqg
This project provides a random query generator to capture the correctness bugs by comparing results between different versions of Apache Spark.

## Download
```
git clone https://github.com/LinhongLiu/spark-rqg.git
```

## Run DataGenerator to create random data for testing
```
./bin/runDataGenerator \
  --randomizationSeed=10 \
  --maxRowCount=110 --minRowCount=100 \
  --maxColumnCount=200 --minColumnCount=100 \
  --tableCount=10 \
  --testSparkVersion=3.0.0-preview2 --refSparkVersion=3.0.0-preview
# Run ./bin/runDataGenerator --help for more information
```

## Run QueryGenerator to generate queries and run against 2 Spark versions
```
./bin/runQueryGenerator --randomizationSeed=0 \
  --queryCount=10 \
  --testSparkVersion=3.0.0-preview2 --testMaster="local[2]"\
  --refSparkVersion=3.0.0-preview --refMaster=yarn-cluster
# Run ./bin/runQueryGenerator --help for more information
```

## Run queries on a customized spark 
```
./bin/runQueryGenerator --randomizationSeed=0 \
  --queryCount=10 \
  --testSparkVersion=3.0.0-SNAPSHOT --testMaster="local[2]"\
  --testSparkHome="/path/to/spark/spark-3.0.0-SNAPSHOT-bin-hadoop2.7" # just specify a spark home
  --refSparkVersion=3.0.0-preview --refMaster=yarn-cluster
```

## Package project for distribution
```
# create a distributable folder in target/pack and launch scripts for data-generator and query-generator
sbt pack

# create a tar.gz archive in target
sbt packArchive
```
