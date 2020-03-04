# Python使用指南

MLSQL内置python支持。MLSQL Console对Python的支持也很棒，可以直接调试python脚本以及开发python项目。

## 调试脚本以及在MLSQL中使用python

在MLSQL Console 中新建项目python-example, 然后新建脚本test.py。先引入一些依赖：


```python
import ray
from pyjava.api.mlsql import RayContext
import numpy as np;
```

你可以通过如下方式获得RayContext对象：

```python
ray_context = RayContext.connect(globals(),"192.168.217.155:57888")
is_in_mlsql = ray_context.is_in_mlsql
```

is_in_mlsql 是判定该python文件是不是在MLSQL里运行。因为我们调试的时候并没有将Python脚本嵌入到MLSQL中执行。我们提供mock数据：

```python
ray_context.mock_data = [{"content":"jack"},{"content":"jack3"}]

def echo(row):
    row["content"]="jackm"
    return row


buffer = ray_context.foreach(echo)
```

上面的例子表示我们对每条数据进行处理。这些任务会提交给ray集群运行。为了方便调试，我们输出下结果：

```python
if not is_in_mlsql:
  for item in buffer:
    print(item)
```

下面是完整的例子：

```python
import ray
from pyjava.api.mlsql import RayContext
import numpy as np;


ray_context = RayContext.connect(globals(),"192.168.217.155:57888")
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

你可以直接点击console里的运行按钮，得到结果。现在我们希望嵌入到MLSQL中运行：

新建MLSQL脚本jack.mlsql, 第一句使用!pyInclude将python脚本引入：

```sql
!pyInclude python-example.test.py named wow1;
```

`python-example.test.py` 是文件路径，我们通过wow1引用这个脚本。

接着进行python的一些环境设置：

```sql
-- ray 配置
!python env "PYTHON_ENV=export PYTHONIOENCODING=utf8 && source activate dev ";
!python conf "schema=st(field(id,long),field(content,string),field(label,double))";
-- !python conf '''schema={"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"content","type":"string","nullable":true,"metadata":{}},{"name":"label","type":"double","nullable":true,"metadata":{}}]}''';
!python conf "pythonMode=ray";
!python conf "runIn=executor";
!python conf "dataMode=data";
```

第一行设置python的环境变量。第二行设置python处理完后的schema信息。

`pythonMode=ray` 表示使用ray来运行，`runIn=executor` 表示我们写的代码会在executor中运行。`dataMode=data`表示数据会在ray集群里走一圈。

我们通过SQL获取一些数据：

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
!ray on orginal_text_corpus1 py.wow1 named mlsql_temp_table2;

--结果
select * from mlsql_temp_table2 as output;
```

下面是完整的脚本：

```sql


!pyInclude python-example.test.py named wow1;

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
!python env "PYTHON_ENV=export PYTHONIOENCODING=utf8 && source activate dev ";
!python conf "schema=st(field(id,long),field(content,string),field(label,double))";
-- !python conf '''schema={"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},{"name":"content","type":"string","nullable":true,"metadata":{}},{"name":"label","type":"double","nullable":true,"metadata":{}}]}''';
!python conf "pythonMode=ray";
!python conf "runIn=executor";
!python conf "dataMode=data";

-- ray处理数据代码
!ray on orginal_text_corpus1 py.wow1 named mlsql_temp_table2;

--结果
select * from mlsql_temp_table2 as output;
```

前面的例子是对每条数据做处理，如果我希望一次性拿到完整的一个分片数据在做统一处理呢？ 可以采用如下方式：

```sql
from pyjava.api.mlsql import RayContext

ray_context = RayContext.connect(globals(),"192.168.217.155:57888")

def echo(rows):
    for row in rows:
        row["content"]="wow"
        yield row


ray_context.map_iter(echo)

```




