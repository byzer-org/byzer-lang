## 机器学习训练部分

【文档更新日志：2020-04-07】

> Note: 本文档适用于MLSQL Engine 1.6.0-SNAPSHOT/1.6.0 及以上版本。  
> 对应的Spark版本可支持2.3.2/2.4.3/3.0.0-preview2

在上一个章节中，我们学会了使用Python做ETL处理的功能，尽管它也能被用于机器学习。但是缺点
也是显而易见的，Python的环境污染了Hadoop的环境，这可能会带来很多问题。为此，MLSQL 引入
Ray支持来解决这些问题。

实际上，MLSQL和Ray整合后，不仅仅能完成机器学习的工作，也同样能完成
ETL的工作，并且没有那些复杂的环境烦恼，透明的将数据处理和机器学习分隔成两个单独的集群。


在上个例子中，我们是将Python代码直接嵌入到MLSQL代码中的。实际上，MLSQL Console 也支持将其分离，
这可以让使用者更好的进行维护和调试。

## 环境准备

大家可以移步[Ray官网](https://ray.readthedocs.io/en/latest/)了解搭建一个简单的Ray集群。


## 使用Ray做ETL的使用示例
在MLSQL Console 中新建项目python-example, 然后新建脚本test.py。先引入一些依赖：


```python
import ray
from pyjava.api.mlsql import RayContext
import numpy as np;
```

你可以通过如下方式获得RayContext对象：

```python
ray_context = RayContext.connect(globals(),"xxxxxx:xxxxx")
is_in_mlsql = ray_context.is_in_mlsql
```

RayContext 和我们前面提到的PythonContext类似，可以通过它让你的Python代码和MLSQL进行交互。
在本节示例中，你需要将`xxxxxx:xxxxx` 修改为一个真实的Ray地址。

is_in_mlsql 是判定该python文件是不是可以脱离MLSQL代码单独在MLSQL里运行。  
因为我们调试的时候并没有将Python脚本嵌入到MLSQL中执行。为了能模拟在MLSQL中运行，  
我们可以提供mock数据：

```python
ray_context.mock_data = [{"content":"jack"},{"content":"jack3"}]

def echo(row):
    row["content"]="jackm"
    return row


buffer = ray_context.foreach(echo)
```

上面的例子表示我们对每条数据进行处理，处理逻辑很简单，就是将content字段替换为jackm。  
这些任务会提交给ray集群运行。为了方便调试，我们输出下结果：

```python
if not is_in_mlsql:
  for item in buffer:
    print(item)
```

到目前为止，我们已经获得一个完整的python代码：

```python
import ray
from pyjava.api.mlsql import RayContext
import numpy as np;


ray_context = RayContext.connect(globals(),"xxxxxx:xxxxx")
is_in_mlsql = ray_context.is_in_mlsql

ray_context.mock_data = [{"content":"jack"},{"content":"jack3"}]

def echo(row):
    row["content"]="jackm"
    return row


buffer = ray_context.foreach(echo)

if not is_in_mlsql:
  for item in buffer:
    print(item)

```

现在，点击点击console里的运行按钮，可以直接调试观察python脚本的结果。一旦你测试完毕，就可以
将该脚本引入到MLSQL,从而体验真实的数据。

我们新建MLSQL脚本jack.mlsql, 第一句使用!pyInclude将python脚本引入：

```sql
!pyInclude python-example.test.py named testPyScript;
```

`python-example.test.py` 是文件路径，为了方便引用这个被引入的脚本，我们将其取名为testPyScript。

使用Ray前我们也要对做一些配置，这个和前面的章节类似：

```sql
-- ray 配置
!python env "PYTHON_ENV=source activate dev ";
!python conf "schema=st(field(id,long),field(content,string),field(label,double))";
!python conf "pythonMode=ray";
!python conf "runIn=executor";
!python conf "dataMode=data";
```

其中，第一行，第二行我们已经在前面见过了。其中schema还支持Json和 DDL.

```shell
!python conf "pythonMode=ray";
```
这一行标识我们会使用ray来处理数据。

```shell
!python conf "runIn=executor";
```
表示我们写的代码会在executor中运行。可选值为 driver/executor. 也就是说你的Ray会在driver  
或者executor端提交Python代码给Ray集群。

```shell
!python conf "dataMode=data";
```
dataMode表示我们对数据做ETL处理，最后会有大量数据返回给MLSQL并且得到一张新表。 可选值为 data/model.
在本章的另外一个实例里，会显示如何按model来使用。

现在，我们可以在MLSQL中模拟一些数据了：

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
```

然后使用之前的python脚本对这里面每天数据做处理：

```sql
-- ray处理数据代码
!ray on orginal_text_corpus1 py.testPyScript named mlsql_temp_table2;

--结果
select * from mlsql_temp_table2 as output;
```

下面是完整的MLSQL代码：

```sql


!pyInclude python-example.test.py named testPyScript;

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
!python env "PYTHON_ENV=source activate dev ";
!python conf "schema=st(field(id,long),field(content,string),field(label,double))";
!python conf "pythonMode=ray";
!python conf "runIn=executor";
!python conf "dataMode=data";

-- ray处理数据代码
!ray on orginal_text_corpus1 py.testPyScript named mlsql_temp_table2;

--结果
select * from mlsql_temp_table2 as output;
```

## ETL如何以分区为粒度做处理

前面的例子是对每条数据做处理，如果我希望一次性拿到完整的一个分片数据在做统一处理呢？ 可以采用如下方式：

```sql
from pyjava.api.mlsql import RayContext

ray_context = RayContext.connect(globals(),"xxxxxxxx:xxxxxx")

def echo(rows):
    for row in rows:
        row["content"]="wow"
        yield row


ray_context.map_iter(echo)

```

## RayContext更底层的API

`ray_context.foreach` 和 `ray_context.map_iter` 是RayContext提供的一些高阶API，
允许你很轻松的使用Ray集群对数据进行处理。用户总是有很多需求，譬如我希望自己控制数据处理会在
Ray集群的特定机器上处理，亦或是我就是希望在Ray client上处理，而不是分布式执行。这个时候就需要使用一些
低阶API。

在Ray 代码被运行的时候， MLSQL 会提供一些Socket Server供Ray Cluster 拉去数据。这些Socket
Server的数据只能被拉去一次，拉取完之后就会被关闭。我们可以通过RayContext获取这些地址：

```python
data_servers = ray_context.data_servers()
```

如果需要获取这些地址的数据，则可使用如下的代码：

```python
RayContext.collect_from(data_servers)
```

实际上，collect_from的实现如下：

```python
@staticmethod  
def collect_from(servers):
    for shard in servers:
        for row in RayContext.fetch_once_as_rows(shard):
            yield row
```

从collect_from实现，你可以看到我们可以 `RayContext.fetch_once_as_rows` 从每个socket
server 拉取数据。

这意味着通过上面的API，你可以和方便的决定数据如何获取并且如何进行处理。

在返回数据给MLSQL上，RayContext复用的是PythonContext的build_result方法，如下：

```python
ray_context.python_context.build_result(items, 1024)
##或者
context.build_result(items, 1024)
```

值得注意的是，上面的方法只能在client端调用，也就是不能作为ray的remote方法执行。但是
items的结果是可以通过任意方式获取的。

## 如何做模型训练

通常而言，模型训练的结果会是一个模型，用户只需要将模型上传到模型存储服务上，然后返回该地址给MLSQL即可。
一个典型的代码如下：

```sql
-- ray 配置
!python env "PYTHON_ENV=source activate dev ";
!python conf "schema=st(field(url,string))";
!python conf "pythonMode=ray";
!python conf "runIn=executor";
!python conf "dataMode=model";

-- ray处理数据代码
!ray on trainingData '''

import ray
from pyjava import rayfix
from pyjava.api.mlsql import RayContext

ray_context = RayContext.connect(globals(),"xxxxxxx:xxxxx")

data_servers = ray_context.data_servers()
# 在某个Ray节点上获取所有数据并且进行训练,上传模型
@ray.remote
@rayfix.last
def train(servers):
    data = RayContext.collect_from(servers)
    train(data)
    url = upload(model)
    return url

url = ray.get(train.remote(data_servers))
context.build_result([{"url":url}])
''' named model_url_table;

--结果
select * from model_url_table as output;
```

在上面的示例中，我们通过Ray API 找到了一个节点，在该节点我们获取了数据，并且进行训练，然后将训练
得到的模型上传到了模型存储服务里。最后返回了一个URL地址给MLSQL，然后我们可能将这个URL地址发送邮件或者
做其他的事情。这个时候我们的dataMode为model,因为此时Python无需返回大量的数据给MLSQL。

注意因为Ray在Python `exec`方法有一些bug,我们需要额外添加`@rayfix.last`注解，并且需要留意  
该注解的位置是在最底层的。

