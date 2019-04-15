# 如何附带资源文件

前面的案例我们已经提及如何附带资源文件。无论是PythonParallelExt还是PythonAlg，使用方式都是一样的。

首先在where条件里，申明资源名称，以及指向的目录。记住，这里的目录都是HDFS目录。

```sql
run newdata as PythonParallelExt.`${modelPath}`
and scripts="pythonScript" 
and entryPoint="pythonScript"
and condaFile="dependencies"
and `fitParam.0.resource.a`="${HOME}/testStreamingB/testDirA"    -- 指定要加载文件目录
and `fitParam.0.resource.b`="${HOME}/testStreamingB/testDirB"    -- 指定要加载文件目录 
;
```

fitParam.0 在这里没有特别的意义。因为所有的python实例都能拿到该资源文件。

而对于PythonAlg而言，fitParam.0/fitParam.1则代表不同组参数，PythonAlg也会启动多个Python Worker(这里是两个)，
然后都处理相同的数据，但是参数不一样。

这里配置的参数，可以在python中如此使用：

```sql
dir_list_A = os.listdir(mlsql.internal_system_param["resource"]["a"])
dir_list_A = [i for i in dir_list_A if i.endswith(".txt")]
dir_list_B = os.listdir(mlsql.internal_system_param["resource"]["b"])
dir_list_B = [i for i in dir_list_B if i.endswith(".txt")]
```