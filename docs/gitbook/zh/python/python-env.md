# Python环境管理

在不同场景，不同项目，甚至不同的脚本中，我们都可能需要不同的Python环境依赖。
MLSQL通过Conda很好的解决了这个问题，你只要描述你需要的依赖是什么，然后系统会自动创建相应的环境给你运行。

但是，在一个分布式环境里，创建和管理环境有其难点。因为Conda自身无法阻止多个任务并发的创建同一个名字的环境，
这会导致返回一个受损的环境。而在真实的分布式环境里，似乎这种并发创建是难以避免的，这主要体现在：

1. 一个Executor在一个主机上，可能多个task同时需要创建相同的环境。
2. 多个Executor在一个主机上，可能会同时需要创建多个相同的环境。
3. 多个属于不同MLSQL-Engine实例的Executor在相同的主机上，可能会需要同时创建多个相同的环境。

为了解决这个问题，我们提供了一套机制来帮助完成环境管理的问题，我们看看这个先决条件：

## 前提条件

如果MLSQL运行在Local模式下，你只要确保conda环境有即可。如果你需要yarn环境下使用，请确保每个节点都安装有conda

注意，如果你用ROOT权限安装conda,请确保你运行的MLSQL有权限使用conda(譬如创建新环境等等)

否则会出现不可预料错误。

## 注意事项

请在使用任何Python功能之前，都优先使用这个章节介绍的方式提前创建好环境。


## 创建环境

```sql
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

run command as PythonEnvExt.`/tmp/jack` where condaFile="dependencies" and command="create";
......

```
如果要删除，则对应为`remove`。

大家可以看到，我们只要通过一个yaml格式的文本描述我们需要的环境，系统就会根据这个需求自动去创建环境。

你也可以指定通过 `condaYamlFilePath` 参数配置一个conda.yaml的路径，系统会读取该路径的内容来创建环境。

> 我们建议将所有的依赖描述文件写在不同的MLSQL脚本中，管理者可以include需要的环境依赖文件，并且使用PythonEnvExt来创建或者删除它。


PythonEnvExt可以保证每个主机特定环境只会被创建一次。现在，你可以放心的运行你的Python项目了：

```sql
run newdata as PythonParallelExt.`${modelPath}`
and scripts="pythonScript" 
and entryPoint="pythonScript"
and partitionKey="hp_date"
and condaFile="dependencies";
```

我们还没有接触过PythonParallelExt之类的东西，但是不要紧张，有个感觉即可。后面会介绍细节。

> PythonEnvExt同样不能支持多个用户同时并发创建一个环境。所以，创建环境的工作，最好交给管理员来完成。 

如果你没有使用本节介绍的ET PythonEnvExt 去管理和创建环境，而是在任务执行的时候去系统去创建环境，则很可能因为在特定服务器上并发创建环境而失败。

注意: 如果运行时报`Could not find Conda executable at conda`等类似错误（一般发生在train/run方法）,需要在where条件中增加以下配置:

```
-- anaconda3为本地安装路径
and systemParam.envs='''{"MLFLOW_CONDA_HOME":"/anaconda3"}''';
```

备注: 你可以通过`conda env list --json`命令查看创建的conda环境信息

## 获取环境名称

如果你只是希望看某个环境的名称，你可以用下面的代码来实现。

```sql
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

run command as PythonEnvExt.`/tmp/jack` where condaFile="dependencies" and command="name";
```

将command设置为 name即可。

## 小技巧

用户可以事先给集群创建几个环境，然后将依赖的文本暴露出来。比如管理者创建一个脚本env1.mlsql

```sql
set env1='''
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
load script.`env1` as env1;
```

使用者可以Include这个脚本：

```sql
include [projet].`env1.mlsql`;

run newdata as PythonParallelExt.`${modelPath}`
and scripts="pythonScript" 
and entryPoint="pythonScript"
and partitionKey="hp_date"
and condaFile="env1";

```

这样，系统会自动复用已经创建好的环境，迅速而有效。如果每个人都去创建环境，会变得多而难于管理。


