# 单实例运行Python项目

什么场景需要单实例运行Python项目呢？ 算法。部分算法只能单机，比如Sklearn。这个时候我们就需要某个Python进程能够拿到所有数据并且进行处理了。

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


## 案例场景

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






