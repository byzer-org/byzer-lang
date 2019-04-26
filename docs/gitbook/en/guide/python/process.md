# Distribute run Python project In MLSQL

## Prerequisites

If you runs on yarn mode, please make sure you start the MLSQL Engine with follow configuration:

```
-streaming.ps.cluster.enable  should be  enabled.

Please make sure
you have the uber-jar of mlsql placed in
1. --jars
2. --conf "spark.executor.extraClassPath=[your jar name in jars]"

for exmaple:

--jars ./streamingpro-mlsql-spark_2.x-x.x.x-SNAPSHOT.jar
--conf "spark.executor.extraClassPath=streamingpro-mlsql-spark_2.x-x.x.x-SNAPSHOT.jar"

Otherwise the executor will
fail to start and the whole application will fails.
```

If you runs on  Standalone, please send the MLSQL jar to every node and then configure:

```
--conf "spark.executor.extraClassPath=[MLSQL jar path]"
```

## How to use

