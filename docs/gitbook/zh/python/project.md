# Python项目

MLSQL 当前支持两种类型的Python项目。

1. 和MLSQL协同工作的的Python项目
2. 独立可执行的Python项目

## 和MLSQL协同工作的的Python项目

MLSQL除了支持前面提到的脚本处理，还支持复杂的Python项目。为了让MLSQL能够识别出你的项目，需要有两个额外的描述文件：

1. MLproject
2. conda.yaml 

一个比较典型的目录结构如下：

```
examples/sklearn_elasticnet_wine/
├── MLproject
├── batchPredict.py
├── conda.yaml
├── predict.py
├── train.py
```

### MLProject

一个典型的MLProject文件是这样的：

```yaml
name: tutorial

conda_env: conda.yaml

entry_points:
  main:
    train:        
        command: "python train.py"
    batch_predict:                  
        command: "python batchPredict.py"
    api_predict:        
        command: "python predict.py"
```

MLProject 指定了项目名称以及conda.yaml描述文件。
同时指定了该项目的入口。

一般我们会指定train/batch_predict/api_predict 其中的一项或者多项。当你有一个机器学习项目，并且需要部署成API服务的时候，才会需要三个入口都指定。
如果仅仅是使用Python做数据处理，只需要配置train入口即可。

所以MLProject其实是一个描述文件，指明在什么场景跑什么脚本。

### conda.yaml

我们其实在前面已经见过，这个是一个标准的conda环境描述文件。

```
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
```


### 使用案例（一）

假设你有个python项目叫pj1, 你需要按前面章节的要求添加MLproject 和 conda.yaml文件。接着你需要上传这个项目到hdfs上。

conda.yaml文件内容如下：

```
name: tutorial
dependencies:
  - python=3.6
  - pip
  - pip:
    - --index-url https://mirrors.aliyun.com/pypi/simple/
    - kafka==1.3.5
    - numpy==1.14.3

```

MLproject文件内容如下：

```sql
name: tutorial

conda_env: conda.yaml

entry_points:
  main:
    train:        
        command: "python train.py"

```

假设项目里只有一个python文件，也就是train.py,内容如下：

```sql
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

    dir_list_A = os.listdir(mlsql.internal_system_param["resource"]["a"])
    dir_list_A = [i for i in dir_list_A if i.endswith(".txt")]
    dir_list_B = os.listdir(mlsql.internal_system_param["resource"]["b"])
    dir_list_B = [i for i in dir_list_B if i.endswith(".txt")]
    with open(tempModelLocalPath + "/result.txt", "w") as f:
        f.write(str(len(dir_list_A) + len(dir_list_B)))

```

现在该如何整合到MLSQL脚本里呢？

```sql
set modelPath="/testStreamingB";

-- 创建依赖环境
select 1 as a as fakeTable;
run fakeTable as PythonEnvExt.`/tmp/jack` where condaYamlFilePath="${HOME}/testStreamingB" and command="create";

load csv.`/testStreamingB` as testData;

run testData as PythonAlg.`${modelPath}`
where pythonScriptPath="${HOME}/testStreamingB"    -- python 项目所在目录
and `fitParam.0.resource.a`="${HOME}/testStreamingB/testDirA"    -- 指定要加载文件目录
and `fitParam.0.resource.b`="${HOME}/testStreamingB/testDirB"    -- 指定要加载文件目录
and keepVersion="false";    -- 是否覆盖模型, true为不覆盖, false为覆盖

load text.`/testStreamingB/model/0` as output;

```

PythonAlg模块，主要就是为了给集合算法使用的。testData里的所有数据都会在`mlsql.internal_system_param["tempDataLocalPath"]`
目录中，你可以获取这些数据然后进行使用，最后训练得到的模型就可以放到`mlsql.internal_system_param["tempModelLocalPath"]`中。

后面章节我们会详解介绍如何使用PythonAlg模块进行训练和预测，还有包括部署成API服务。

### 使用案例（二）

很多场景，我们有部分逻辑处理是用python脚本来完成，可能是因为python某个库，也可能就是自己不愿意写SQL，写UDF函数，
而且最好是通过

1. 读取文件
2. 处理
3. 写入文件

三步走，因为这个模式大家都熟悉。

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



##  独立可执行的Python项目

MLSQL Console支持普通的Python项目。创建python-project1,然后创建package `tech.mlsql.example.echo.py`,
为了确保这是一个包名，你还需要创建一个`tech.mlsql.example.__init__.py`文件。
同样的，我们创建一个 `tech.mlsql.main.py`,请保证每个目录都有一个__init__.py文件。

其中echo.py内容为：

```
def echo(word):
  return word
```

在main.py中你可以应用该echo方法：

```
import sys
from pyjava.api.mlsql import PythonProjectContext

from tech.mlsql.example.echo import echo
print(echo("中国"))
```
现在你可以运行main.py,系统可以正确的识别这个python项目。

在MLSQL中，Python中的print都会被输出到Console端中，你也可以显示的将一些东西输出：

```
context = PythonProjectContext()
context.log_client.log_to_driver("-----")
```

支持独立可执行的Python项目允许用户在MLSQL Console中完成一个复杂的Python项目的开发。并且未来提供
自动化的二进制部署功能。

如果需要设置项目的运行环境，可以你需要在MLSQL中执行如下指令：

```
!python env "PYTHON_ENV=source activate dev ";
```

这表明，当前用户默认的python环境是dev.


