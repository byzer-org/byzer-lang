# jsonStr/script/mlsqlAPI/mlsqlConf

MLSQL 内置了一些特殊的数据源，比如jsonStr主要是为了加载通过set语法得到的json字符串，然后解析成表。
script则是为了加载一些脚本成为表，方便后续引用。

## jsonStr/script
jsonStr我们前面已经遇到多次：

```sql
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';

load jsonStr.`rawData` as table1;
```

这个对于调试异常方便。当然也可以作为模板来使用。

script使用方式和jsonStr类似，我们来看一个例子：

```sql

-- 定义了一段python脚本，之后需要通过script load下进行使用

set python_script='''
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

load script.`python_script` as python_script;

run testData as PythonParallelExt.`${modelPath}`
where scripts="python_script"
and entryPoint="python_script"
and condaFile="...."; 
```

只有通过script 进行load之后才能方便的被后面的语句引用。后面的文档中我们会看到更多的
使用示例。

## mlsqlAPI/mlsqlConf

mlsqlAPI 可以查看所有API,mlsqlConf可以查看所有启动脚本参数。具体使用如下：

```
load mslqlAPI.`` as api_table;
load mlsqlConf.`` as confg_table;
```

## csvStr

同`jsonStr`，脚本数据源也支持`csvStr`。例如：

```sql
set rawData='''
name,age
zhangsan,1
lisi,2
''';
load csvStr.`rawData` options header="true"
as output;
```
