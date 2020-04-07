## Hello World示例

```sql

-- 示例数据
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


-- python 配置
!python env "PYTHON_ENV=source activate dev ";
!python conf "schema=st(field(content,string))";

-- python处理
!python on orginal_text_corpus1 '''

data = context.fetch_once_as_rows()
def process(data):
    for row in data:
        new_row = {}
        new_row["content"] = "---" + row["content"]+"---"
        yield new_row

context.build_result(process(data))

''' named mlsql_temp_table2;

--结果
select * from mlsql_temp_table2 as output;
```

我们重点关注 【python配置】 以及 【python处理】部分。

在【python配置】部分中：

1. env 指定我们当前需要运行的python环境。这里我们选择了dev.也就是前面我们安装的环境的。
2. conf 中设定schema为 st(field(content,string))，该schema描述的是python处理完后的结果。对于该schema描述语法,参考项目[simple-schema](https://github.com/allwefantasy/simple-schema)

schema除了支持simple-schema描述以外，还支持Json或者DDL格式。

Json:

```shell
!python conf '''schema={"type":"struct","fields":[{"name":"content","type":"string","nullable":true,"metadata":{}}]}''';
```

DDL:

```shell
!python conf '''schema=content STRING''';
```

多个字段按逗号分隔即可。

在【python处理】处理不分，遵循如下语法：

```
!python on [table name] [python code] named [new table name]
```

比如下面的代码，

```
!python on orginal_text_corpus1 '''

data = context.fetch_once_as_rows()
def process(data):
    for row in data:
        new_row = {}
        new_row["content"] = "---" + row["content"]+"---"
        yield new_row

context.build_result(process(data))

''' named mlsql_temp_table2;
```

我们可以做这样的解读：

对表orginal_text_corpus1的数据，我们使用上面示例中的python代码对标进行并行处理。  
处理后得到的结果为一张新表，该表名字为 mlsql_temp_table2。

> 值得注意的是，上面的python代码会被多个python worker执行，然后分别对表中的一部分数据进行处理。

根据上面的解读，我们要写的python代码其实是对表orginal_text_corpus1的数据进行处理，然后输出成新表。
既然如此，在写python代码的时候，我们需要有一个途径，允许我们获取数据，并且最后将处理好的数据输出出去。
pyjava项目提供了满足输入输出的接口。入口是context对象，它的全名是：`pyjava.api.PythonContext`.

通过 `context.fetch_once_as_rows()` 获得表的数据，然后不通过`context.build_result(...)`返回处理好的数据.
除此之外，我们大部分情况下不需要和pyjava的其他接口打交道。

这个示例，可以很方便我们使用Python对数据做ETL,但是缺点也很明显：

1. 要求driver以及executor都安装了了特定的python环境。
2. 分布式执行，如果只允许python代码被一个python进程执行，那么就需要对处理的表先进行重新分区。
3. 在Executor执行Python worker可能对Hadoop集群带来稳定性影响。

为此，我们还是建议用户使用Ray模式。

## 关于使用Python处理MLSQL表的一些原理

当我们使用Python处理一张表的数据时，我们会在表分区的节点启动相应的Python Workers,并且我们会一直复用他们。
对于这些Python Workers，我们可以通过如下方式限制Worker的大小。

```sql
!python conf "py_executor_memory=600";
```

上面的例子表示我们会限制Python的worker内存大小不超过600m.
值得注意的是，无论`!python env`还是`!python conf`都是session级别有效的。这意味着一旦设置之后，会影响当前用户后续所有的操作。
