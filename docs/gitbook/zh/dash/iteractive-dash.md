# 制作一张交互式报表

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


完整代码如下：

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

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.express as px
import ray
from dash.dependencies import Input, Output


ray_context = RayContext.connect(globals(),"192.168.209.29:42207")
data = list(ray_context.collect())

APP_NAME="jack"
APP_PORT="8051"

@ray.remote
class DashServer(object):
    def __init__(self,port):
        self.port = port
        self.app_dash = dash.Dash(APP_NAME, external_stylesheets=["https://codepen.io/chriddyp/pen/bWLwgP.css"])
    
    def shutdown(self):
        ray.actor.exit_actor()
    
    def get_address(self):
        return ray.services.get_node_ip_address()    

    def start_server(self):
        from flask import request
        tips = px.data.tips()
        col_options = [dict(label=x, value=x) for x in tips.columns]
        dimensions = ["x", "y", "color", "facet_col", "facet_row"]
        app = self.app_dash
        app.layout = html.Div(
            [
                html.H1("Demo: Plotly Express in Dash with Tips Dataset"),
                html.Div(
                    [
                        html.P([d + ":", dcc.Dropdown(id=d, options=col_options)])
                        for d in dimensions
                    ],
                    style={"width": "25%", "float": "left"},
                ),
                dcc.Graph(id="graph", style={"width": "75%", "display": "inline-block"}),
            ]
        )

        @app.callback(Output("graph", "figure"), [Input(d, "value") for d in dimensions])
        def make_figure(x, y, color, facet_col, facet_row):
            return px.scatter(
                tips,
                x=x,
                y=y,
                color=color,
                facet_col=facet_col,
                facet_row=facet_row,
                height=700,
            )

        def shutdown_server():
            func = request.environ.get('werkzeug.server.shutdown')
            if func is None:
                raise RuntimeError('Not running with the Werkzeug Server')
            func()

        @app.server.route('/shutdown', methods=['GET', 'POST'])
        def shutdown():
            shutdown_server()
            return 'Server shutting down...'
        print(self.get_address())
        app.run_server(self.get_address(),self.port)

def actor_exits(name):
    try:
        ray.experimental.get_actor(name)
        exists = True
    except ValueError:
        exists = False
    return exists    


if actor_exits(APP_NAME):
    dash_server = ray.experimental.get_actor(APP_NAME)
    try:
        ray.get(dash_server.shutdown.remote())
    except Exception:
        pass
    from ray.experimental.named_actors import _calculate_key 
    worker = ray.worker.get_global_worker()
    worker.redis_client.delete(_calculate_key(APP_NAME))      




dash_server = DashServer.options(name=APP_NAME, detached=True, max_concurrency=2).remote(APP_PORT)
host = ray.get(dash_server.get_address.remote())
dash_server.start_server.remote()

context.build_result([{"content":"http://{}:{}".format(host,APP_PORT)}])

''' named mlsql_temp_table2;

select content as url,"" as dash from mlsql_temp_table2 as output;
```

这个代码详细的展示MLSQL了如何结合Ray + Dash + Plotly 制作一张交互报表. 最后展示在MLSQL  
Console里的是一个URL地址，访问该地址，得到如下报表：

![](http://docs.mlsql.tech/upload_images/WX20200414-124022.png)

我们可以自己动态选择一些参数从而将数据渲染成不同的形态。
