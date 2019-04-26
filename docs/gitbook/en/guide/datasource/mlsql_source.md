# jsonStr/script/mlsqlAPI/mlsqlConf

MLSQL has some built-in datasource. For example, jsonStr can load  json string from set statement.
and then convert it as table.


## jsonStr/script

```sql
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';

load jsonStr.`rawData` as table1;
```

jsonStr is convenient for debugging.

script is like jsonStr:

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


## mlsqlAPI/mlsqlConf

mslqlAPI is used to list api.
mlsqlConf is used to list startup parameters.

```
load mslqlAPI.`` as api_table;
load mlsqlConf.`` as confg_table;
```

## csvStr

csvStr is load csv string from set statement.

```sql
set rawData='''
name,age
zhangsan,1
lisi,2
''';
load csvStr.`rawData` options header="true"
as output;
```
