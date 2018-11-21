## Prerequisites

1. java 1.8  or later

## Installation

```
git clone https://github.com/allwefantasy/streamingpro .
cd streamingpro

./dev/make-distribution.sh


cp streamingpro-bin-1.1.3.tgz /tmp
cd /tmp && tar xzvf  streamingpro-bin-1.1.3.tgz
cd /tmp/streamingpro

export SPARK_HOME="....." ; ./start-local.sh
```

## Manually Installation

Step 1: Download the jars from the release page: [Release页面](https://github.com/allwefantasy/streamingpro/releases):

1. streamingpro-mlsql-1.x.x.jar
2. ansj_seg-5.1.6.jar
3. nlp-lang-1.7.8.jar

Step 2:

Visit the downloads page: [Spark](https://spark.apache.org/downloads.html), download Apache Spark 2.2.0 and then unarvhive it.

Step 3:

```shell
cd spark-2.2.0-bin-hadoop2.7/

./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[*] \
--name sql-interactive \
--jars ansj_seg-5.1.6.jar,nlp-lang-1.7.8.jar \
streamingpro-mlsql-1.1.2.jar    \
-streaming.name sql-interactive    \
-streaming.job.file.path file:///tmp/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift false \
-streaming.enableHiveSupport true
```

`query.json` is a json file contains "{}".

Step 4: visit http://127.0.0.1:9003


![](https://github.com/allwefantasy/mlsql-web/raw/master/images/WX20180629-105204@2x.png)


## Docker support(For testing only)

1. build mlsql base docker image.

```
docker build -t mlsql-base:v1 dev/docker
```

2. build runnable mlsql binary package.

```
dev/make-distribution.sh
```

3. decompression binary package.


```
tar zxf streamingpro-bin-1.1.3.tgz
```

4. start mlsql server.

```
docker run -it -v ${PWD}:/app -p 9003:9003 mlsql-base:v1 /app/streamingpro/start-local.sh
```

5. visit http://127.0.0.1:9003



## Build from Source

```
git clone https://github.com/allwefantasy/streamingpro.git

cd streamingpro

mvn -DskipTests clean package \
-pl streamingpro-mlsql \
-am  \
-Ponline \
-Pscala-2.11 \
-Phive-thrift-server \
-Pspark-2.3.0 \
-Pdsl  \
-Pshade \
-Pcrawler \
-Pxgboost  \
-Pstreamingpro-spark-2.3.0-adaptor \
-Pcarbondata
````

Warning: that -Pautoml is not capable with -Pcarbondata.

When you use spark-2.4.x:

|profile name   | required  | description  |   |   |
|---|---|---|---|---|
|online                                | true  |   |   |   |
|scala-2.11                            | true  |   |   |   |
|shade                                 | true  |   |   |   |
|hive-thrift-server                    | false |   |   |   |
|spark-2.4.0                           | true  |   |   |   |
|dsl                                   | true  |   |   |   |
|crawler                               | true  |   |   |   |
|automl                                | false  |   |   |   |
|xgboost                               | false  |   |   |   |
|streamingpro-spark-2.4.0-adaptor      | true  |   |   |   |
|carbondata                            | false  |   |   |   |
|opencv-support                        | false  |   |   |   |




When you use spark-2.3.x:

|profile name   | required  | description  |   |   |
|---|---|---|---|---|
|online                                | true  |   |   |   |
|scala-2.11                            | true  |   |   |   |
|shade                                 | true  |   |   |   |
|hive-thrift-server                    | false |   |   |   |
|spark-2.3.0                           | true  |   |   |   |
|dsl                                   | true  |   |   |   |
|crawler                               | true  |   |   |   |
|automl                                | false  |   |   |   |
|xgboost                               | false  |   |   |   |
|streamingpro-spark-2.3.0-adaptor      | true  |   |   |   |
|carbondata                            | false  |   |   |   |
|opencv-support                        | false  |   |   |   |



When you use spark-2.2.x:


|profile name   | required  | description  |   |   |
|---|---|---|---|---|
|online                                | true  |   |   |   |
|scala-2.11                            | true  |   |   |   |
|shade                                 | true  |   |   |   |
|hive-thrift-server                    | false  |   |   |   |
|spark-2.2.0                           | true  |   |   |   |
|dsl-legacy                            | true  |   |   |   |
|crawler                               | true  |   |   |   |
|automl                                | false  |   |   |   |
|xgboost                               | false  | no support  |   |   |
|streamingpro-spark-2.2.0-adaptor      | true  |   |   |   |
|carbondata                            | false  |   |   |   |
|opencv-support                        | false  |   |   |   |


spark-1.6.x/scala-2.10 will not be maintained, we will remove it soon.


