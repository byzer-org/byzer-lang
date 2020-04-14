# 制作一张动态报表

【文档更新日志：2020-04-14】

> Note: 本文档适用于MLSQL Engine 1.6.0-SNAPSHOT/1.6.0 及以上版本。  
> 对应的Spark版本可支持2.3.2/2.4.3/3.0.0-preview2
>

##  前提

我们假设你已经拥有了[MLSQL机器学习指南（Python版）](http://docs.mlsql.tech/zh/python/) 相关的一些知识。


##  环境要求：

```
pip install Cython
pip install pyarrow==0.10.0
pip install ray==0.8.0
pip install aiohttp psutil setproctitle grpcio
pip install watchdog requests click uuid sfcli  pyjava 
pip install dash plotly
```


利用前面我们获得数据，现在可以使用这些数据然后使用python绘制报表了：

```sql
load delta.`python_data.vega_datasets` as vega_datasets;

!python env "PYTHON_ENV=source activate dev";
!python conf "schema=st(field(content,string))";

select * from vega_datasets where Year > 1990 as dash_data;

!ray on dash_data '''

import pandas as pd
import plotly.express as px
from plotly.io import to_html
from vega_datasets import data
from pyjava.api.mlsql import PythonContext,RayContext

ray_context = RayContext.connect(globals(),None)
data = list(ray_context.collect())
df = pd.DataFrame(data, columns=data[0].keys())

fig = px.bar(df,
             y="Entity",
             x="Deaths",
             animation_frame="Year",
             orientation='h',
             range_x=[0, df.Deaths.max()],
             color="Entity")
# improve aesthetics (size, grids etc.)
fig.update_layout(width=1000,
                  height=800,
                  xaxis_showgrid=False,
                  yaxis_showgrid=False,
                  paper_bgcolor='rgba(0,0,0,0)',
                  plot_bgcolor='rgba(0,0,0,0)',
                  title_text='Evolution of Natural Disasters',
                  showlegend=False)
fig.update_xaxes(title_text='Number of Deaths')
fig.update_yaxes(title_text='')


html = to_html(
    fig,
    config={},
    auto_play=False,
    include_plotlyjs=True,
    include_mathjax="cdn",
    post_script=None,
    full_html=True,
    animation_opts=None,
    default_width="50%",
    default_height="50%",
    validate=False,
)

context.build_result([{"content":html}])

''' named mlsql_temp_table2;

select content as html,"" as dash from mlsql_temp_table2 as output;
```

渲染结果如下：

![](http://docs.mlsql.tech/upload_images/WechatIMG80.png)

因为我们并不需要真的去连接一个Ray集群，我们直接在client执行，所以可以将Ray的URL地址甚至为
None:

```python
ray_context = RayContext.connect(globals(),None)
```

我们可以通过如下代码获取表dash_data所有的数据：

```python
data = list(ray_context.collect())
```

`ray_context.collect()` 返回的是generator,所以需要转化为一个list来使用。

最后，绘制的图被渲染成html,我们需要将值回传出去：

```python
context.build_result([{"content":html}])
```

build_result接受一个list,里面是一个map.

为了能够让MLSQL Console进行渲染，我们需要使用如下的语句：

```sql
select content as html,"" as dash from mlsql_temp_table2 as output;
```

核心是，一个标记指端dash,表示这个表可以被渲染成图标，一个内容字段，html,表示把内容直接当做html进行渲染。








