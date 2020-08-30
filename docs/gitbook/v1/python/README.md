# MLSQL Python支持

MLSQL 以一种非常创新的方式支持Python,从而完成ETL以及算法训练，预测。

1. Python代码既可以以字符串形式内嵌到MLSQL中，也可以单独成脚本Include进MLSQL中。
2. Python对数据处理的方式也是表进表出。这意味着你可以使用Python对数据进行ETL也可以做算法训练，预测等等，没有任何约束。
3. 无缝整合Ray,用户可以使用Ray API 进行分布式计算，亦或是机器学习训练预测。


##典型示例

下面代码分别对应控制台的两个脚本:

![](http://docs.mlsql.tech/upload_images/17372336-9ffb-4565-bef0-daa1ec9c1699.png)

大家可以看看MLSQL是对他们如何进行有机整合的。

### foreach.mlsql

```sql
-- 加载python脚本
!pyInclude ray_data.foreach.py named wow1;

-- 加载并且处理数据
load delta.`public.consumer_complains` as cc;
select * from cc 
union all select * from cc 
union all select * from cc 
union all select * from cc 
as newcc;

-- ray 配置
!python env "PYTHON_ENV=source activate dev";
!python conf "schema=st(field(ProductName,string),field(SubProduct,string))";
!python conf "runIn=driver";
!python conf "dataMode=data";

-- ray处理数据代码
!ray on newcc py.wow1 named mlsql_temp_table2;

--结果
save overwrite mlsql_temp_table2 as parquet.`/tmp/mlsql_temp_table2`;
load  parquet.`/tmp/mlsql_temp_table2` as mlsql_temp_table3;

```

### foreach.py

```python
import ray
from pyjava.api.mlsql import RayContext
import numpy as np;

ray_context = RayContext.connect(globals(),"192.168.209.29:49417")

def echo(row):
    row1 = {}
    row1["ProductName"]="jackm"
    row1["SubProduct"] = row["SubProduct"]
    return row1
    

ray_context.foreach(echo)

```