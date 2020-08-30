# 数据处理

在MLSQL中，你可以先用SQL处理数据，然后接着再对数据使用Python处理。使用者可以非常的灵活。
在Console中，MLSQL文件和Python文件可以分开写：

![](http://docs.mlsql.tech/upload_images/bef6c174-c25c-4137-b533-048c8874212e.png)

当然，也可以直接把Python代码内嵌到MLSQL脚本里。

## SQL加载和预处理数据

下面的代码我们加载了数据湖里的测试数据。

```
load delta.`public.example_data` as cc;
select * from cc 
-- union all select * from cc 
-- union all select * from cc 
-- union all select * from cc 
as newcc;
```

得到表newcc。

## 配置Python以及使用Python代码处理数据

如果你使用conda,那么可以用下面的指令指定需要使用的环境（如果没有使用，则不需要这句命令）：

```shell
-- ray 配置
!python env "PYTHON_ENV=source /usr/local/miniconda/bin/activate dev";
```

接着指定Python代码是在Driver还是在Executor端跑，推荐Driver跑。

```shell
!python conf "runIn=driver";
```

现在指定python的数据返回格式：

```shell
!python conf "schema=st(field(ProductName,string),field(SubProduct,string))";
```

如果用户使用Ray做分布式数据处理，请将dataMode设置为data.否则设置为model. 在现在这个例子里，我们设置为model，因为我们只是简单的在python中获取所有数据，然后处理返回。

```
!python conf "dataMode=model";
```

在MLSQL中加载python脚本：

```
!pyInclude python_example.foreach.py named wow1;
```

对应的目录结构如下：

![](http://docs.mlsql.tech/upload_images/aa9b7b3f-dfc0-4e53-9100-e842fc0e0267.png)


加载后就可以使用Python对表进行处理了：

```
!ray on newcc py.wow1 named mlsql_temp_table2;
```

最后保存结果并且加载显示：

```
--结果
save overwrite mlsql_temp_table2 as parquet.`/tmp/mlsql_temp_table2`;
load  parquet.`/tmp/mlsql_temp_table2` as mlsql_temp_table3;
```


## Python脚本说明

前面我们用到的 python_example/foreach.py 的内容如下：

```python
## 引入必要的包
import ray
from pyjava.api.mlsql import RayContext
import numpy as np;

## 获取ray_context,如果需要使用Ray，那么第二个参数填写Ray地址
## 否则设置为None就好。
ray_context = RayContext.connect(globals(),None)

# 通过ray_context.data_servers() 获取所有数据源，如果开启了Ray， 那么就可以在
# 分布式获取这些数据进行处理。
datas = RayContext.collect_from(ray_context.data_servers())

## 对数据进行处理
def echo(row):
    row1 = {}
    row1["ProductName"]="jackm"
    row1["SubProduct"] = row["SubProduct"]
    return row1
    

buffer = (echo(row) for row in datas)

## 输出结果. 其中context 是内置变量，无需申明就可以使用。
context.build_result(buffer)
```

## 如何使用Ray分布式处理

前面的例子我们其实没有使用到Ray，这里我们简单介绍下用户如何使用pyjava高阶API自动使用Ray完成分布式处理：

```python
import ray
from pyjava.api.mlsql import RayContext
import numpy as np;

## 做分布式处理，第二个Ray地址参数是必须的
ray_context = RayContext.connect(globals(),"192.168.209.29:49417")

def echo(row):
    row1 = {}
    row1["ProductName"]="jackm"
    row1["SubProduct"] = row["SubProduct"]
    return row1
    
ray_context.foreach(echo)
```

除了foreach,还有map_iter， map_iter 里的函数接受一个generator,返回也需要是一个generator.

在[PyJava API简介](http://docs.mlsql.tech/mlsql-engine/python/pyjava.html),我们会更详细的介绍各种API，方便用户获取和处理数据。


