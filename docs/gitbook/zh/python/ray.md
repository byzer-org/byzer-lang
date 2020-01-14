## Ray支持

MLSQL默认Python支持，是让Python进城和底层Spark引擎在同一个集群。比如可能是共享了Yarn集群，但是Python环境并不好管理，资源控制也会是个问题，总体而言，
运维成本高，对Hadoop压力大，不支持GPU调度等相关功能。所以MLSQL在1.5.0版本对Ray做了支持。大家可以简单将Ray理解为一个分布式Python执行引擎。


## 环境配置

MLSQL会启动一个Python Client去和Ray交互，所以你需要保证Python Client和Ray所处的Python环境是一致的。MLSQL为了简化环境，允许将client运行在
Driver端，这样你只要配置一个Driver端的Python环境即可，可通过下面的命令设置：

```
!python conf "runIn=driver";
```

接着，你需要在driver以及Ray上安装如下Python包：

```
pip install aiohttp psutil setproctitle grpcio  pyarrow==0.12.0 ray pandas numpy pyjava
```

如果你还需要用到一些其他的包，可以自行安装。

## 数据处理

这里，我们将Ray当做一个数据处理集群。比如针对表的每一行做处理。

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

## 一条一条处理
def echo(row):
    return row

ray_context.setup(echo)
ray_context.foreach(echo) # pyjava >= 0.2.6.1

## 或者一个shard一个shard处理，pyjava >= 0.2.6.1
def echo(rows):
    for row in rows:
        yield row

ray_context.map_iter(echo) 


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

## 这里你可以启动ray的一个task进行模型训练啥的
@ray.remote
@rayfix.last
def echo(item):
    return item
echo_res = echo.remote("jack")
print(ray.get(echo_res))

## 下面是通过较为底层的API获取数据，用户可以使用ray的task并行完成：

rows = []
for shard in ray_context.data_servers():    
    for item in ray_context.fetch_once_as_rows(shard):
        rows.append(item)

## 设置结果返回给系统
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

