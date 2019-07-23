# 分布式运行Python项目

很多场景，我们有部分逻辑处理是用python脚本来完成，可能是因为python某个库，也可能就是自己不愿意写SQL，写UDF函数，
而且最好是通过

1. 读取文件
2. 处理
3. 写入文件

三步走，因为这个模式大家都熟悉。

## 前提条件

如果MLSQL运行在Local模式下，你只要确保conda环境有即可。如果你需要yarn环境下使用，请确保每个节点都安装有conda,并且
确保启动脚本中按如下要求进行设置：


```

-streaming.ps.cluster.enable  should be  enabled.

Please make sure
1. you have the uber-jar of mlsql placed in --jars
2. Put mlsql-ps-service_xxx_2.11-xxx.jar to $SPARK_HOME/libs 

More detail about [mlsql-ps-service](https://github.com/allwefantasy/mlsql-ps-service)

Otherwise the executor will
fail to start and the whole application will fails.

```

否则会出现不可预料错误。



## 使用步骤

MLSQL支持在脚本中完成以上逻辑。我们来具体看看如何实现.  

第一步，我们先用set语法写一个python脚本：

```sql
-- 要执行的python代码
set pythonScript='''
import os
import warnings
import sys

import mlsql

if __name__ == "__main__":
    warnings.filterwarnings("ignore")

    tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]

    isp = mlsql.params()["internalSystemParam"]
    tempModelLocalPath = isp["tempModelLocalPath"]
    if not os.path.exists(tempModelLocalPath):
        os.makedirs(tempModelLocalPath)
    with open(tempModelLocalPath + "/result.txt", "w") as f:
        f.write("jack")
''';
```

从脚本我们看到，这是一个非常标准的Python本地文件操作。通过 `mlsql.internal_system_param["tempDataLocalPath"]` 我们输入目录，
通过 `mlsql.internal_system_param["tempModelLocalPath"]` 我们拿到输出目录。这就是MLSQL和python脚本之间所有的约定了。

在tempDataLocalPath中，会有很多json文件，你需要过滤出后缀名为`.json`的文件，然后读取和处理。处理完成之后，你把处理的结果用json的方式
写回 tempModelLocalPath。当然，不是json的也行，但是后面就不太好用了。

第二步，用set语法描述下这个python脚本需要依赖哪些库，并且让同自动创建这些依赖的环境：

```sql
set dependencies='''
name: tutorial
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
select 1 as a as fakeTable;
run fakeTable as PythonEnvExt.`/tmp/jack` where condaFile="dependencies" and command="create";

```

其中kafka和pyspark是必须要的，因为mlsql依赖它。其他的大家自己根据需求配置就好。这个示例使用了阿里云镜像。

第三部，注册和使用：

```sql
set modelPath="/tmp/jack2";

set data='''
{"jack":1}
''';

load jsonStr.`data` as testData;
load script.`pythonScript` as pythonScript;

run testData as RepartitionExt.`` where partitionNum="5" as newdata;    --partitionNum=5即将数据分成5个分区

run newdata as PythonParallelExt.`${modelPath}`
and scripts="pythonScript" 
and entryPoint="pythonScript"
and condaFile="dependencies";


```

如果你有多个脚本文件，那么可以在`scripts`参数中用","进行分割即可。计算完成后，在

`/tmp/jack2/model`下你会看到有0-4五个目录，每个目录里有一个json（你先前写的文件）。

但是你会发现读取这些文件是一个麻烦的事情，因为有五个目录，你可以这么做添加一个参数：

```
and partitionKey="hp_date"
```

里面hp_date 是分区字段。这样你接着就可以这样读取你的计算结果：

```
load json.`/tmp/jack2/model` as output;
```

假设我还希望能够有一些资源文件怎么办？比如我希望在python脚本里完成分词,这个时候需要有词典，你可以这样用：

```sql
run newdata as PythonParallelExt.`${modelPath}`
and scripts="pythonScript" 
and entryPoint="pythonScript"
and condaFile="dependencies"
and `fitParam.0.resource.a`="${HOME}/testStreamingB/testDirA"    -- 指定要加载文件目录
and `fitParam.0.resource.b`="${HOME}/testStreamingB/testDirB"    -- 指定要加载文件目录 
;
```

接着你在python脚本里就可以通过如下方式获得配置的资源文件：

```python
    
dir_list_A = os.listdir(mlsql.internal_system_param["resource"]["a"])
dir_list_A = [i for i in dir_list_A if i.endswith(".txt")]
dir_list_B = os.listdir(mlsql.internal_system_param["resource"]["b"])
dir_list_B = [i for i in dir_list_B if i.endswith(".txt")]

```

是不是非常方便？





