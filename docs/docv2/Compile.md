## Compile

Compile Example(Spark 2.3.x):

```
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
-Pautoml \
-Pxgboost  \
-Pstreamingpro-spark-2.3.0-adaptor \
-Pcarbondata \
-Popencv-support
````


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
|xgboost                               | false  |   |   |   |
|streamingpro-spark-2.2.0-adaptor      | true  |   |   |   |
|carbondata                            | false  |   |   |   |
|opencv-support                        | false  |   |   |   |


spark-1.6.x/scala-2.10 will not be maintained, we will remove it soon.

## Docker runtime support

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
