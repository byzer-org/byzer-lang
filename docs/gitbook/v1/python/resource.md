# Python资源限制

在MLSQL中的Python代码会单独在Driver或者Executor节点上启动一个Python进程运行。默认总数量不超过节点的核数。

不过遗憾的是，如果不注意控制Python进程的资源占用，而MLSQL Engine又跑在K8s(Yarn上也是类似情况)上，很可能导致容器被杀，如果是driver节点被杀，那么会导致整个Engine失败。为了避免这种情况：

1. 连接Ray集群并且将处理逻辑都放到Ray里去完成（官方推荐）
2. 对Python代码所处的进程做资源限制

对于1，我们可以使用RayContext.foreach/RayContext.map_iter 做处理。这样可以保证数据的交互无需经过client。

对于2，用户可以在容器里设置环境变量 `export PY_EXECUTOR_MEMORY=300` 亦或是 `!python conf "py_executor_memory=300";` 这里表示Python 内存不应该超过300M. 尽管如此，第二种方案还是有缺陷，如果你有8核，当多个用户并行使用时，最多会占用2.4G内存，很可能导致容器被杀死。

对于方式一，下面是一个典型的例子：

```python
load delta.`public.consumer_complains` as cc;
select * from cc limit 100
-- union all select * from cc 
-- union all select * from cc 
-- union all select * from cc 
as newcc;

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


ray_context = RayContext.connect(globals(),"192.168.209.29:12879")


def echo(rows):
    for row in rows:
      row1 = {}
      row1["ProductName"]="jackm"
      row1["SubProduct"] = row["SubProduct"]
      yield row1
    

ray_context.map_iter(echo)
''' named mlsql_temp_table2;

--结果
save overwrite mlsql_temp_table2 as parquet.`/tmp/mlsql_temp_table2`;
load  parquet.`/tmp/mlsql_temp_table2` as mlsql_temp_table3;
```

ray_context.map_iter 会保证你的数据处理逻辑都运行在Ray集群上。对于上面的模式，仍然有个细节需要了解，就是map_iter里函数运行的次数，取决于数据表`newcc`的分片数。如果你希望在函数里拿到所有数据，那么可以将newcc 的分片数设置为1。

将newcc分片数设置为1的方式有两种：

1. 

