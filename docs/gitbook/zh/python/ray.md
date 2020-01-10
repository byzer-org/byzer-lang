## Ray支持

MLSQL支持使用Ray来做数据处理和模型训练。

> 使用前，请确保在Ray的Python环境里安装了pyjava，同时也确保在MLSQL的driver以及executor端的Python环境里安装了pyjava.

一个可选定安装环境的命令如下：

```
pip install aiohttp psutil setproctitle grpcio  pyarrow==0.12.0 ray pandas numpy
```


数据处理参考如下代码：

```sql

set rawText='''
{"id":9,"content":"1","label":0.0}
{"id":10,"content":"2","label":0.0}
{"id":11,"content":"中国","label":0.0}
{"id":12,"content":"e","label":0.0}
{"id":13,"content":"5","label":0.0}
{"id":14,"content":"4","label":0.0}
''';

load jsonStr.`rawText` as orginal_text_corpus;

select id,content,label from orginal_text_corpus as orginal_text_corpus1;

!desc orginal_text_corpus1 json;

-- ray 配置
!python env "PYTHON_ENV=export PYTHONIOENCODING=utf8 && export MLSQL_DEV=1 && source activate streamingpro-spark-2.4.x";
!python conf "schema=st(field(id,long),field(content,string),field(label,double))";
-- !python conf '''schema={"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"content","type":"string","nullable":true,"metadata":{}},{"name":"label","type":"double","nullable":true,"metadata":{}}]}''';
!python conf "pythonMode=ray";
!python conf "runIn=driver";
!python conf "dataMode=data";

-- ray处理数据代码
!ray on orginal_text_corpus1 '''

from pyjava.api.mlsql import RayContext

ray_context = RayContext.connect(context,"192.168.31.80:10579")

def echo(row):
    return row


ray_context.setup(echo)


''' named mlsql_temp_table2;

--结果
select * from mlsql_temp_table2 as output;
```

模型参考如下代码：

```sql
set rawText='''
{"id":9,"content":"1","label":0.0}
{"id":10,"content":"2","label":0.0}
{"id":11,"content":"中国","label":0.0}
{"id":12,"content":"e","label":0.0}
{"id":13,"content":"5","label":0.0}
{"id":14,"content":"4","label":0.0}
''';

load jsonStr.`rawText` as orginal_text_corpus;

select id,content,label from orginal_text_corpus as orginal_text_corpus1;

-- !desc orginal_text_corpus string;

!python env "PYTHON_ENV=export PYTHONIOENCODING=utf8  && source activate streamingpro-spark-2.4.x";
!python conf "schema=st(field(id,long),field(label,double))";
!python conf "pythonMode=ray";
!python conf "runIn=driver";
!python conf "dataMode=model";

!ray on orginal_text_corpus1 '''

import pandas as pd
import numpy as np
import os
from pyjava import rayfix
from pyjava.api.mlsql import RayContext
import ray
import sys

ray_context = RayContext.connect(context,"192.168.206.35:30312")



@ray.remote
@rayfix.last
def echo(item):
    return item
echo_res = echo.remote("jack")
print(ray.get(echo_res))


context.set_output([[
pd.Series([1, 2, 3, 4]),
pd.Series([2.0, 3.0, 4.0, 5.0])
]])

## data_manager.set_output([])

''' named mlsql_temp_table2;

select * from mlsql_temp_table2 as output;

```          

关于配置：


选择ray client的环境：


```
!python env "PYTHON_ENV=export PYTHONIOENCODING=utf8  && source activate streamingpro-spark-2.4.x";
```

ray输出的数据的schema,也可以是json格式：

```
!python conf "schema=st(field(id,long),field(label,double))";
```

python模式是ray：

```
!python conf "pythonMode=ray";
```

ray的client跑在driver端：

```
!python conf "runIn=driver";
```

和ray的交互模式是数据处理还是模型训练：

```
!python conf "dataMode=model";
```
model表示模型训练，data表示数据处理。

