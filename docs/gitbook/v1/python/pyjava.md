#PyJava API简介

前面所有的Python文件里都引入了pyjava包，

```python
from pyjava import rayfix
from pyjava.api.mlsql import RayContext
```

该包实现了Python和系统的交互。

1. 系统内置了一个context,类型为PythonContext.
2. 大部分情况下，用户需要使用RayContext

## 初始化RayContext

通过如下代码初始化RayContext：

```python
ray_context = RayContext.connect(globals(),"192.168.31.80:17019")
```

其中第二个参数是可选的，如果不连接Ray集群，则设置为None即可。

## 获取数据分片

通过下面代码可以获取多个数据地址：

```python
data_servers = ray_context.data_servers()
```

data_servers是字符串数组，每个元素是一个 `ip:port`的形态. 从这些数据地址，用户可以获取下列代码中

```sql
load delta.`public.example_data` as cc;
!ray on cc py.wow1 named mlsql_temp_table2;
```
cc 表的数据。

有了地址之后，MLSQL提供了多种方式去拉取真实的数据。

最简单的模式：

```sql
RayContext.collect_from(ray_context.data_servers())
```

该代码会遍历每个地址，然后返回一个generator.  

如果已经连接了Ray,那么可以直接使用高阶API `ray_context.foreach`

```python
def echo(row):
    row1 = {}
    row1["ProductName"]="jackm"
    row1["SubProduct"] = row["SubProduct"]
    return row1
    

buffer = ray_context.foreach(echo)
```
foreach接受一个回调函数，函数的入参是一条记录。用户无需显示的申明如何获取数据，只要实现回调函数即可。我们也可以获得一批数据，可以使用`ray_context.map_iter`。 系统会自动调度多个任务到Ray上并行运行。 map_iter会根据表的分片大小启动相应个数的task,如果你希望通过map_iter拿到所有的数据，而非部分数据，可以先对表做重新分区：

```
-- simpleDataTemp转化成只有一个分片数据的新表simpleData; 
!tableRepartition _ -i simpleDataTemp -num 1 -o simpleData;
```

一个使用map_iter的例子：

```python


!pyInclude ray_data.foreach.py named wow1;

-- load delta.`public.consumer_complains` as cc;
-- select * from cc 
-- union all select * from cc 
-- union all select * from cc 
-- union all select * from cc 
-- as newcc;

-- save overwrite newcc as parquet.`/tmp/newcc`  where fileNum="1";
load parquet.`/tmp/newcc` as newcc_temp;
!tableRepartition _ -i newcc_temp -num 1 -o newcc;
-- select count(*) from newcc as output;

-- ray 配置
!python env "PYTHON_ENV=source activate dev";
!python conf "schema=st(field(ProductName,string),field(SubProduct,string))";
!python conf "runIn=driver";
!python conf "dataMode=data";

-- ray处理数据代码
!ray on newcc '''
import ray
from pyjava.api.mlsql import RayContext
import numpy as np;
import time


ray_context = RayContext.connect(globals(),"xxxx:xxxx")


def echo(rows):
    count = 0
    for row in rows:
      row1 = {}
      row1["ProductName"]="jackm"
      row1["SubProduct"] = row["SubProduct"]
      count = count + 1
      if count%1000 == 0:
          print("=====> " + str(time.time()) + " ====>" + str(count))
      yield row1
    

ray_context.map_iter(echo)


''' named mlsql_temp_table2;

--结果
save overwrite mlsql_temp_table2 as parquet.`/tmp/mlsql_temp_table2`;
load  parquet.`/tmp/mlsql_temp_table2` as mlsql_temp_table3;

```

如果用户希望自己掌控任务的调度，那么可以使用Ray 的remote方法

```python
data_servers = ray_context.data_servers()

# 对每个分区的数据进行处理
@ray.remote
@rayfix.last
def process(server):
    import time
    time.sleep(1)
    data = RayContext.fetch_once_as_rows(server)
    items = [{"content":item['content']} for item in data]
    return items

# 启动N个任务进行处理
items = [ray.get(process(server)) for server in servers]
```

在这里，用户可以使用`RayContext.fetch_once_as_rows(server)` 获取一个特定分区的数据。

## 获取数据并且存储到python运行的主机

通过 `ray_context.collect_as_file` 你就可以获取一个文件引用，然后可以反复迭代数据。

```python
data = ray_context.collect_as_file(2)
for item in data: 
    print(item)
```

其中里面的参数表示，你需要迭代几次的数据量。算法需要对数据进行多次训练，该参数就是为了满足这个需求。








