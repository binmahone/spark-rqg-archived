# spark-rqg

A random query generator for finding correctness bugs by comparing results
between different versions or configurations of Apache Spark.

## Table of Contents
- [spark-rqg](#spark-rqg)
  - [Table of Contents](#table-of-contents)
  - [Download](#download)
  - [Quickstart for Photon Users](#quickstart-for-photon-users)
- [User Guide](#user-guide)
  - [Generating Random Data](#generating-random-data)
  - [Running Random Query Generator](#running-random-query-generator)
    - [Reproducibility](#reproducibility)
    - [Comparing against a local Spark](#comparing-against-a-local-spark)
  - [RQG Output](#rqg-output)
    - [Using the Manifest File](#using-the-manifest-file)
    - [Reproducing Query Output](#reproducing-query-output)
    - [Detailed Output](#detailed-output)
  - [Supported Spark Versions](#supported-spark-versions)
  - [Running RQG with Photon Failure Injection](#running-rqg-with-photon-failure-injection)
    - [RQG Results with Failure Injection](#rqg-results-with-failure-injection)
    - [Troubleshooting](#troubleshooting)

## Download

```
git clone https://github.com/databricks/spark-rqg.git
```

## Quickstart for Photon Users

```bash
cd $HOME/runtime
# Build local DBR checkout with Bazel.
bazel build //repl:repl-hive-2.3__hadoop-2.7_2.12 //sql/hive:hive-unshaded_2.12 --config debug
export BAZEL_SPARK_SUBMIT="$(cat bazel-bin/sql/hive/hive-unshaded_2.12.runtimeclasspath.txt | tr '\n' ':')"
export BAZEL_SPARK_SUBMIT="$BAZEL_SPARK_SUBMIT:$(cat bazel-bin/repl/repl-hive-2.3__hadoop-2.7_2.12.runtimeclasspath.txt | tr '\n' ':')"

# Generate Parquet data for RQG
cd $HOME/spark-rqg
./bin/runDataGenerator\
  --randomizationSeed=10\
  --maxRowCount=10000 --minRowCount=5000\
  --maxColumnCount=100 --minColumnCount=50\
  --tableCount=10\
  --dataSources=PARQUET\
  --configFile=conf/rqg-photon-defaults.conf


# Run against reference Spark (by default, this is Apache Spark v3.1.1)
./bin/runQueryGenerator --randomizationSeed=10 --queryCount=20\
    --testSparkVersion=spark-with-photon\
    --testSparkHome=$HOME/runtime\
    --useParquet=true\
    --configFile=conf/rqg-photon-defaults.conf
```

See [this section](#using-the-manifest-file) for how to parse the output and
[this section](#reproducing-query-output) for how to quickly reproduce errors.

# User Guide

## Generating Random Data

Generates some random data and writes it to `$HOME/spark_rqg_data`. If this directory
already exists, the contents will be overwritten.

```bash
./bin/runDataGenerator\
  --randomizationSeed=10\
  --maxRowCount=10000 --minRowCount=5000\
  --maxColumnCount=100 --minColumnCount=50\
  --tableCount=10
# Run ./bin/runDataGenerator --help for more information
```

## Running Random Query Generator

Run some random queries. As an example:

```bash
./bin/runQueryGenerator --randomizationSeed=234 --queryCount=10
# Run ./bin/runQueryGenerator --help for more information
```

This will download two reference Spark versions and compare them to each other.

### Reproducibility

Randomly generated data and queries are reproduceable across runs with (1) the same random seed and
(2) the same `rqg-defaults.conf` file.

### Comparing against a local Spark
You can also run a local Spark version, or download a different version. To run a local
version of Spark, just build it with `sbt` first, with Hive support:

```bash
cd /path/to/spark
build/sbt -Phive -Phive-thriftserver clean package
```

Then point the query generator to the spark directory. The following example uses a
local version of Spark as the test version and downloads `spark-3.0.0` as the reference:

```bash
./bin/runQueryGenerator --randomizationSeed=10 --queryCount=10\
    --testSparkVersion=spark-with-photon\
    --testSparkHome=$HOME/runtime\
    --refSparkVersion=spark-3.0.0
```

[Downloadable versions can be found here](https://archive.apache.org/dist/spark/).

## RQG Output

The main output for a particular run of the RQG is at `$HOME/rqg_output/rqg-<uuid>/manifest.txt`.
This file lists each query's SQL, its log files, and status (`PASS`, `MISMATCH`, `EXCEPTION`, `CRASH`,
or `SKIPPED` due to crash). For mismatches and exceptions, the manifest also prints the diff between
the reference and test.

The UUID is printed during execution and is the `currentTimeMillis()` of when the run started.

### Using the Manifest File

Finding and triaging issues in an RQG run is easy with standard command line tools.

```bash
# Find all crashing queries
cat $HOME/rqg_output/rqg-<uuid>/manifest.txt | grep "- CRASH"

# Count the number of queries executed.
cat $HOME/rqg_output/rqg-<uuid>/manifest.txt | grep "Query Status" | wc -l

# Count the number of passing queries
cat $HOME/rqg_output/rqg-<uuid>/manifest.txt | grep "- PASS" | wc -l
```

The manifest file is also structured in a way that makes it easy to consume programmatically. The
current format is:

```
----! Query Status for rqg-<uuid>/batch-<batch-id>/<absolute-query-num> - <status>
Reference Spark Log4J: <filename>
Test Spark Log4J: <filename>
----! Query SQL
<sql-text>
----! Error Message
<error specific message>
```

### Reproducing Query Output

Before the RQG runs, it sets up the test database in the directory of the specified Spark
installations. Reproducing RQG issues is thus as easy as:

```bash
cd /path/to/tested/spark
bin/spark-shell <options-used-with-rqg>
```

to run a query, copy and paste it from the manifest file and run it in `spark-shell` as follows:

```scala
scala> spark.sql(queryText)
```

### Detailed Output

All output for a particular run of the RQG is written to a directory under
`$HOME/rqg_output/rqg-<uuid>`. This includes Log4J logs for the RQG itself, Log4J logs
for each `spark-submit` job, the queries, and the result golden files.

The RQG submits queries to Spark in _batches_. For a single batch, the RQG will:
1. Generate random queries to execute
2. Submit those queries to the reference Spark and test Spark with `spark-submit`
3. Compare the results of the reference and test Spark

The RQG writes the output for a given batch to `$HOME/rqg_output/rqg-<uuid>/batch-<batch-num>`. If
Spark throws an exception or crashes altogether, information about the crash can be found in this
directory. The RQG continues executing new batches after a crash, unless configured not to do so
with the `--stopOnCrash=true` option.

## Supported Spark Versions

The RQG currently supports all Spark versions compiled by Scala 2.12. For example:
* Pre-built Spark 3.0 download from [Apache](https://archive.apache.org/dist/spark/)
* Locally compiled Spark 2.4.x with Scala 2.12

Note that the RQG uses `spark-submit` to submit Spark jobs: you may need some manual setup to
support this for your distribution.

## Running RQG with Photon Failure Injection

You can run RQG with failure injection enabled in Photon.
All the existing instructions still apply, but the only change is how you build the Photon library.
The following steps will build Photon with failure injection

```bash
# Build Photon with failure injection enabled.
cd $HOME/runtime
bazel build //photon:lib-photon.so --config debug --config finject --copt="-DFAILURE_INJECTION_PROBABILITY=20"
sudo mkdir -p /databricks/native
sudo ln -sf $PWD/bazel-bin/photon/lib-photon.so /databricks/native/lib-photon.so

# Run against reference Spark (by default, this is Apache Spark v3.0.1)
./bin/runQueryGenerator --randomizationSeed=10 --queryCount=20\
    --testSparkVersion=spark-with-photon\
    --testSparkHome=$HOME/runtime
```

### RQG Results with Failure Injection

After RQG completes, the output will look something like this:

```
Finished executing 2000 queries.

1254 queries passed
  - 580 passing queries produced meaningful data
0 queries produced incorrect results compared to reference
746 queries crashed or threw exceptions
0 queries skipped due to crash in batch
```

Since failure injection is enabled, it is expected that queries fail.
The goal for failure injection RQG testing is to see if there are any unexpected Exceptions,
DCHECKs, or crashes due to the injected failures.

If you inspect the manifest file, all the queries are expected to have the status `PASS` or
`EXCEPTION`.
You can easily see all the query statuses which are not `PASS` with:
```bash
cat $HOME/rqg_output/rqg-<uuid>/manifest.txt | grep "! Query Status" | grep -v PASS
```

The queries with status `EXCEPTION` should all be due to failure injection, and nothing else.
In the manifest file, the exception message will always contain `Injected failure ...` for those
injected failures. For example, a failed query due to an injected failure will contain:
```
...
== Actual Answer ==
schema: struct<>
output: java.lang.RuntimeException
Injected failure into ReadImpl at photon/data-layout/io/compression.cc:168
    @     0x7f10b3d0302a  photon::GetInjectedFailureMsg()
    @     0x7f10bb161552  photon::LZ4FInputStream::ReadImpl()
    @     0x7f10bb0a271f  photon::InputStream::Read()
    @     0x7f10bb0a501e  photon::ReadFromInput()
    @     0x7f10bb0ab17d  photon::TypedColumnReader<>::ReadValues()
    @     0x7f10bb0a8e77  photon::TypedColumnReader<>::ReadAndAppendColumn()
    @     0x7f10bb0a741c  photon::TypedColumnReader<>::ReadAndAppendChildren()
    @     0x7f10bb0f38d6  photon::TypedColumnReader<>::ReadAndAppendColumn()
    @     0x7f10bb0a2368  photon::ColumnBatchReader::ReadAndAppendColumns()
    @     0x7f10bb0a1d34  photon::ColumnBatchReader::Read()
    @     0x7f10b43a73d3  photon::ShuffleFileReader::ReadBatch()
    @     0x7f10b3e32828  photon::ShuffleExchangeSourceNode::HasNext()
    @     0x7f10b3dc7ab5  photon::GroupingAggNode::Open()
    @     0x7f10b3cff0e5  Java_com_databricks_photon_JniApiImpl_open
    @     0x7f12cd018427  (unknown)
```

In order to find any Exception in the manifest file which are NOT due to injected failures you can
use the following `grep` command:

```bash
cat $HOME/rqg_output/rqg-<uuid>/manifest.txt | grep -Pzo 'Query Status for .*EXCEPTION\n(((?!Query Status for).)*\n)*?== Actual Answer ==\nschema: .*\noutput: .*Exception\n(((?!Injected failure).)*\n)*?----! '
```

This will display the query information for queries which failed with Exceptions, but not due to
injected failures.

### Troubleshooting
If building runtime with SBT fails, cleaning up your dependency caches and git checkout might fix it.
```
build/sbt clean package
rm -rf ~/.m2/
rm -rf ~/.ivy2/cache/
git clean -dxf
```
