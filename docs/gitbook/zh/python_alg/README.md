# Python算法

我们之前已经讲述了如何在MLSQL中使用Python.在本章节，我们会重点探讨如何使用Python做机器学习。 同前面章节一样，你需要满足下面描述的
前提条件。

## 前提条件

如果MLSQL运行在Local模式下，你只要确保conda环境有即可。如果你需要yarn环境下使用，请确保每个节点都安装有conda,并且
确保启动脚本中按如下要求进行设置：

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

否则会出现不可预料错误。