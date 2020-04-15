# Python环境管理

【文档更新日志：2020-04-14】

> Note: 本文档适用于MLSQL Engine 1.6.0-SNAPSHOT/1.6.0 及以上版本。  
> 对应的Spark版本可支持2.3.2/2.4.3/3.0.0-preview2
>


##  环境准备

MLSQL 实例所在的服务器的Python的环境管理其实是一个大难题。MLSQL经历了两年的使用积累，对此提出了一些实践规则。

1. 推荐在MLSQL中使用Ray。这样MLSQL实例只要求Driver有合适的Python环境即可。并且，我们推荐在Driver上使用Conda进行管理。
2. 如果你无法使用Ray,那么我们强烈建议用户在所有机器上安装Conda以方便环境的管理，并且预先创建几个稳定的Python环境供用户使用。
3. 如果算法使用者需要自己创建环境，那么可以使用PythonEnvExt来进行相关辅助管理。

我们之所以推荐用户使用Ray,是因为通常在Yarn上维护和管理Python环境，并且运行Python程序是一件
困难并且危险的事情的。因为Yarn通常也和HDFS的进程混放在一起，如果Python进程占用了过量的
的CPU和内存资源，这会导致HDFS,也就是我们的存储不稳定。通过Ray,我们会仅仅在Driver或者Executor
端启动一个client,client会非常的轻量，实际的计算会放到Ray Cluster完成。


如果大家使用方案1，并且确定只在Driver端使用Python,那么你仅仅需要在Driver端以及Ray Cluster里  
安装如下库：

```
pip install Cython
pip install pyarrow==0.10.0
pip install ray==0.8.0
pip install aiohttp psutil setproctitle grpcio
pip install watchdog requests click uuid sfcli  pyjava
```

如果是方案2,3 那么你需要手动或者使用PythonEnvExt在机器上安装如上的库。假设我们安装了上面
库的conda环境叫做dev. 我们后面的例子都会使用环境dev.

## 注意

如果MLSQL运行在Local模式下，你只要确保conda环境有即可。如果你需要yarn环境下使用，请确保每个节点都安装有conda

注意，如果你用ROOT权限安装conda,请确保你运行的MLSQL有权限使用conda(譬如创建新环境等等)

否则会出现不可预料错误。


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

通过上面的命令，我们可以得到一个新的环境，系统创建完成后，会返回一个环境名字给你，格式为 【mlflow-UUID】。

> PythonEnvExt同样不能支持多个用户同时并发创建一个环境。所以，创建环境的工作，最好交给管理员来完成。 

如果你没有使用本节介绍的ET PythonEnvExt 去管理和创建环境，而是在任务执行的时候去系统去创建环境，则很可能因为在特定服务器上并发创建环境而失败。

注意: 如果运行时报`Could not find Conda executable at conda`等类似错误（一般发生在train/run方法）,需要在where条件中增加以下配置:

```
-- anaconda3为本地安装路径
and systemParam.envs='''{"MLFLOW_CONDA_HOME":"/anaconda3"}''';
```

备注: 你可以在具体的服务器上通过`conda env list --json`命令查看创建的conda环境信息

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
