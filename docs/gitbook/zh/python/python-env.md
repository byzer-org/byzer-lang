# Python环境管理

在不同场景，我们需要不同的Python环境依赖。MLSQL通过Conda很好的解决了这个问题，你只要描述你需要的依赖是什么，然后系统会自动创建相应的环境给你运行。
但是，Conda自身无法阻止多个任务并发的创建同一个名字的环境，这会导致返回一个受损的环境，所以在分布式场景下，环境的管理是较为困难的，难点主要体现在：

1. 一个Executor在一个主机上，可能多个task同时需要创建相同的环境。
2. 多个Executor在一个主机上，可能会同时需要创建多个相同的环境。
3. 多个属于不同MLSQL-Engine实例的Executor在相同的主机上，可能会需要同时创建多个相同的环境。

虽然如此，MLSQL还是提供了一个非常简洁的管理办法，比如，你需要运行一个Python项目，在运行之前，你可以先执行如下语句：

```sql
......

run fakeTable as PythonEnvExt.`/tmp/jack` where condaFile="dependencies" and command="create";
......

```

这里，condaFile是你前面依赖文本的引用：

```
set dependencies='''
name: tutorial4
dependencies:
  - python=3.6
  - pip
  - pip:
    - --index-url https://mirrors.aliyun.com/pypi/simple/
    - numpy==1.14.3
    - kafka==1.3.5
    - pyspark==2.3.2
    - pandas==0.22.0
''';

load script.`dependencies` as dependencies;
```

而command则表示删除或者创建这套环境。如果要删除，则对应为`remove`。

你也可以指定通过 `condaYamlFilePath` 参数配置一个conda.yaml的路径，系统会读取该路径的内容来创建环境。

> 我们建议将所有的依赖描述文件写在不同的MLSQL脚本中，管理者可以include需要的环境依赖文件，并且使用PythonEnvExt来创建或者删除它。



现在你就可以运行你的python项目了：

```sql
run newdata as PythonParallelExt.`${modelPath}`
and scripts="pythonScript" 
and entryPoint="pythonScript"
and partitionKey="hp_date"
and condaFile="dependencies";
```

PythonEnvExt可以保证每个主机特定环境只会被创建一次。

> PythonEnvExt同样不能支持多个用户同时并发创建一个环境。所以，创建环境的工作，最好交给管理员来完成。 

如果你没有使用本节介绍的ET PythonEnvExt 去管理和创建环境，而是在任务执行的时候去系统去创建环境，则很可能因为在特定服务器上并发创建环境而失败。

注意: 如果运行时报`Could not find Conda executable at conda`等类似错误（一般发生在train/run方法）,需要在where条件中增加以下配置:

```
-- anaconda3为本地安装路径
and systemParam.envs='''{"MLFLOW_CONDA_HOME":"/anaconda3"}''';
```

备注: 你可以通过`conda env list --json`命令查看创建的conda环境信息



