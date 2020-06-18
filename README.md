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
./bin/runQueryGenerator --randomizationSeed=10 \
  --queryCount=10 \
  --testSparkVersion=3.0.0-preview2 \
  --refSparkVersion=3.0.0-preview
# Run ./bin/runQueryGenerator --help for more information
```

```
Output:
Comparing queries 0 to 9
Comparing query 0: SELECT DISTINCT ((table_alias_0.column_51) + (table_alias_0.column_70))  AS expr_alias_1, ((first((table_alias_0.column_88) )) - (table_alias_0.column_88))  AS expr_alias_2 FROM table_1 AS table_alias_0    GROUP BY expr_alias_1, table_alias_0.column_88   LIMIT 68
PASS.
Comparing query 1: SELECT DISTINCT (1187760176)  AS expr_alias_2, (table_alias_1.column_40)  AS expr_alias_3, (- (table_alias_0.column_53))  AS expr_alias_4, ((sum((0.98129654) )) + (table_alias_0.column_41))  AS expr_alias_5 FROM table_10 AS table_alias_0 INNER JOIN table_5 AS table_alias_1 ON (((table_alias_0.column_130) >= (table_alias_1.column_99)) ) OR ((false) ) WHERE ((- (table_alias_0.column_63)) > (table_alias_0.column_101))   GROUP BY expr_alias_2, expr_alias_3, expr_alias_4, table_alias_0.column_41
PASS.
Comparing query 2: SELECT DISTINCT ((table_alias_0.column_53) - (table_alias_0.column_3))  AS expr_alias_1, ((first((table_alias_0.column_88) )) + (994224767))  AS expr_alias_2, (-6302721630776663050)  AS expr_alias_3, (((446906847) + (-767901457)) + (table_alias_0.column_77))  AS expr_alias_4 FROM table_1 AS table_alias_0    GROUP BY expr_alias_1, expr_alias_3, expr_alias_4
PASS.
Comparing query 3: SELECT DISTINCT (89)  AS expr_alias_2, (7798260648579902401)  AS expr_alias_3, (sum((table_alias_1.column_35) ))  AS expr_alias_4, (+ (+ (2469894411)))  AS expr_alias_5, (first(('UlCz62ARKJCi') ))  AS expr_alias_6 FROM table_8 AS table_alias_0 INNER JOIN table_4 AS table_alias_1 ON ((table_alias_0.column_6) >= (table_alias_1.column_123))    GROUP BY expr_alias_2, expr_alias_3, expr_alias_5
PASS.
Comparing query 4: SELECT DISTINCT (-7290215689)  AS expr_alias_2, (sum((table_alias_1.column_70) ))  AS expr_alias_3, (+ (first((table_alias_1.column_73) )))  AS expr_alias_4, (abs((-51191232) ))  AS expr_alias_5 FROM table_10 AS table_alias_0 LEFT OUTER JOIN table_7 AS table_alias_1 ON (((table_alias_0.column_111) >= (table_alias_1.column_89)) ) OR ((table_alias_0.column_81) ) WHERE (table_alias_0.column_146)   GROUP BY expr_alias_2, expr_alias_5   LIMIT 55
PASS.
Comparing query 5: SELECT DISTINCT ((table_alias_0.column_94) - (7417411916))  AS expr_alias_1, (+ (count((1926663866) )))  AS expr_alias_2, (- (count((table_alias_0.column_72) )))  AS expr_alias_3 FROM table_6 AS table_alias_0    GROUP BY expr_alias_1   LIMIT 0
PASS.
Comparing query 6: SELECT DISTINCT (- (first((table_alias_1.column_90) )))  AS expr_alias_3, (~ ((6713469257012530263) - (table_alias_2.column_131)))  AS expr_alias_4 FROM table_6 AS table_alias_0 RIGHT OUTER JOIN table_2 AS table_alias_1 ON ((table_alias_0.column_118) >= (table_alias_1.column_54))  INNER JOIN table_6 AS table_alias_2 ON NOT (((table_alias_1.column_73) != (table_alias_2.column_65)) )   GROUP BY expr_alias_4
PASS.
Comparing query 7: SELECT DISTINCT (table_alias_0.column_162)  AS expr_alias_1, (- (sum((table_alias_0.column_156) )))  AS expr_alias_2 FROM table_5 AS table_alias_0    GROUP BY expr_alias_1   LIMIT 90
PASS.
Comparing query 8: SELECT DISTINCT (table_alias_0.column_59)  AS expr_alias_2, (1195653742)  AS expr_alias_3, (first((-1969465414) ))  AS expr_alias_4 FROM table_6 AS table_alias_0 LEFT OUTER JOIN table_9 AS table_alias_1 ON (((table_alias_0.column_75) < (table_alias_1.column_145)) ) AND ((false) )   GROUP BY expr_alias_2, expr_alias_3   LIMIT 46
PASS.
Comparing query 9: SELECT DISTINCT (count((table_alias_0.column_183) ))  AS expr_alias_1, ((6641165158) + (-4338456829))  AS expr_alias_2, (.08464761633596518)  AS expr_alias_3, (table_alias_0.column_118)  AS expr_alias_4 FROM table_10 AS table_alias_0    GROUP BY expr_alias_2, expr_alias_3, expr_alias_4   LIMIT 17
PASS.
```

## Run queries on a customized spark 
```
./bin/runQueryGenerator --randomizationSeed=0 \
  --queryCount=10 \
  --testSparkVersion=3.0.0-SNAPSHOT --testMaster="local[2]"\
  --testSparkHome="/path/to/spark/spark-3.0.0-SNAPSHOT-bin-hadoop2.7" # just specify a spark home
  --refSparkVersion=3.0.0-preview2 --refMaster="local[2]"
```

## Supported Spark Versions
Currently we support all spark versions compiled by scala 2.12. For example:
* Pre-built Spark 3.0 Preview download from apache
* Self complied Spark 2.4.x with scala 2.12

## Package project for distribution
```
# create a distributable folder in target/pack and launch scripts for data-generator and query-generator
sbt pack

# create a tar.gz archive in target
sbt packArchive
```

## Best way to resolve some issues 

Delete any related rqg generated folder like rqg_data and spark_rqg in the home directory.
Delete the /tmp/test-spark/ folder.
Then, run the dataGenerator and queryGenerator commands again.

## FAQ

### How to solve the problem of `spark-submit returned with exit code x, See the log4j logs for more detail`?

The Spark RQG use SparkSubmit to run queries and collect results, for this error log, we need to check the detailed exception for SparkSubmit, all the running logs should be located in `spark-rqg/logs`.

### How to deal with the download error?

If the test/refSparkHome isn't set, Spark RQG will download the test/refSparkVersion automatically, just retry the whole command for the downloading error. You can also use your own local Spark version instead of downloading them from maven repo, any local Spark code directory with a successful compilation is acceptable.

### How to handle the failure of `Cannot receive any reply from xxxx:xxxx`?

Sometimes the user will receive this error for the first starting, try to set `SPARK_LOCAL_IP` env to `127.0.0.1` to solve the problem.

### Why I met the scala version related error while using Spark RQG?

It's because we use Scala 2.12 as the default Scala version in RQG to be consistent with Spark 3.0 or newer version. But if you use the existing Spark version before 3.0, you'll get such exception since they used Scala 2.11. To fix this, you need to recompile the old version by 2.12 or change both test and ref version to before or after 3.0.

### Table meta does not exist
We can easily solve this by running the dataGenerator command

