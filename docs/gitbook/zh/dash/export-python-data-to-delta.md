# 把python测试数据集导出到数仓

 【文档更新日志：2020-04-14】

> Note: 本文档适用于MLSQL Engine 1.6.0-SNAPSHOT/1.6.0 及以上版本。  
> 对应的Spark版本可支持2.3.2/2.4.3/3.0.0-preview2
>

##  前提

我们假设你已经拥有了[MLSQL机器学习指南（Python版）](http://docs.mlsql.tech/zh/python/) 相关的一些知识。


实际场景，我们的数据一般都是会在数仓里。为了模拟这个实际情况，这篇内容会介绍如何将python的数据
导入到Hive仓库里。

完整脚本如下：

```
!python env "PYTHON_ENV=source activate dev";
!python conf "schema=st(field(Entity,string),field(Year,long),field(Deaths,long))";
!python conf "dataMode=model";

!ray on command '''

import plotly.express as px
from plotly.io import to_html
from vega_datasets import data

df = data.disasters()

context.set_output([[df[name] for name in df]])

''' named mlsql_temp_table2;

save overwrite mlsql_temp_table2 as delta.`python_data.vega_datasets`;

```

简单做个说明，前面三行主要是配置Python的一些参数。其中第二行为数据的格式描述。为了获得vega_datasets，
用户可以在MLSQL Console里新建一个python脚本，比如叫`vega_datasets.py`,然后执行如下代码：

```
import plotly.express as px
from plotly.io import to_html
df = px.data.gapminder()
print(df.iloc[0])
```

会输出如下内容：

```
 Year                       1900
 Name: 0, dtype: object
 Deaths                  1267360
 Entity    All natural disasters
 Entity    All natural disasters
 Name: 0, dtype: object
 Year                       1900
 Deaths                  1267360
```

这样我们就知道数据结构，从而写出schema了。接着我们使用内嵌python代码获得数据，其中

`!ray on command` 中的command表示这个我们并不需要一个真实的数据集。

而最后设置输出的代码：

```python
context.set_output([[df[name] for name in df]])
```

可以看到我们需要拿到df的所有列得到一个数组，然后在套一个数组即可。这个是典型的列式传输的模式。