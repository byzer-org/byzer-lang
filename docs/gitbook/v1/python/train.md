# 模型训练

下面是一个模型训练的python代码：

```python
import ray
from pyjava import rayfix
from pyjava.api.mlsql import RayContext

## 如果需要找Ray节点做训练，则第二个参数必须填写
ray_context = RayContext.connect(globals(),"192.168.31.80:17019")

data_servers = ray_context.data_servers()
# 在某个Ray节点上获取所有数据并且进行训练,上传模型
@ray.remote
@rayfix.last
def train(servers):
    import time
    time.sleep(1)
    data = RayContext.collect_from(servers)
    ## 这里对数据进行模型训练
    model_train(data)
    ## 上传模型到模型仓库之类的远程地址
    path = upload_model()
    ## 返回地址
    return path

path = ray.get(train.remote(data_servers))
context.build_result([{"path":path}])
```

理论上用户可以通过Ray API 实现分布式训练。之后可以在MLSQL脚本里引用该脚本进行数据处理。

```sql
load delta.`public.example_data` as cc;
!ray on cc py.wow1 named mlsql_temp_table2;
```
