# 如何统一管理scala/python udf脚本

在前面章节中，我们知道MLSQL允许你在脚本中书写Python,Scala,MLSQL等语言，虽然很灵活，但是所有的东西混杂在一块并不是一个好的
选择。比如下面的代码看起来就会让你头疼：


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

-- 配置文件代码
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

-- 主执行流程
set modelPath="/tmp/jack2";

set data='''
{"jack":1}
''';

load jsonStr.`data` as testData;
load script.`pythonScript` as pythonScript;
load script.`dependencies` as dependencies;

-- 这部分是核心流程
--partitionNum=5即将数据分成5个分区
run testData as RepartitionExt.`` where partitionNum="5" as newdata;    
run newdata as PythonParallelExt.`${modelPath}`
where partitionNum="3"    
and scripts="pythonScript" 
and entryPoint="pythonScript"
and condaFile="dependencies";

```

如果假设有多个python脚本要写，大家会觉得更难受，一个脚本里充斥了大量的python代码。这个时候我么可以玩：
假设我们主文件叫做`main.py`. 

我们新建一个文件夹python，在里面见process.mlsql,内容如下：

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

load script.`pythonScript` as process;
```

接着新建一个 dependency.mlsql,内容如下：

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

load script.`dependencies` as dependency;

```

修改主文件main.sql：

```mlsql
 include project.`python.process`;
 include project.`python.dependency`;
 -- 主执行流程
 set modelPath="/tmp/jack2";
 
 set data='''
 {"jack":1}
 ''';
 
 load jsonStr.`data` as testData;
 
 run testData as RepartitionExt.`` where partitionNum="5" as newdata;    
 run newdata as PythonParallelExt.`${modelPath}`
 where partitionNum="3"    
 and scripts="process" 
 and entryPoint="process"
 and condaFile="dependency";
```

值得注意的是，引用时以load script 后的名字为准。



