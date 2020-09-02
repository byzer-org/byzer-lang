# 结合Python保存Excel

MLSQL 2.0.1-SNAPSHOT/2.0.1 以及以上版本可用

> MLSQL 提供了 [excel插件](https://github.com/allwefantasy/mlsql-plugins/tree/master/mlsql-excel)，可以让用户读写Excel。更多详情参考[插件](http://docs.mlsql.tech/mlsql-engine/plugin/)

## 加载数据

首先加载数据

```sql
load delta.`public.simpleData` as simpleDataTemp;
-- simpleDataTemp转化成只有一个分片数据的新表simpleData; 
!tableRepartition _ -i simpleDataTemp -num 1 -o simpleData;
```

如下：

![](http://docs.mlsql.tech/upload_images/4b50b955-3efe-4ade-beee-dbbac8cf4c3b.png)


## Ray配置

```
-- python处理完后的数据的schema
!python conf "schema=st(field(file,binary))";
-- Python跑在driver端，
!python conf "runIn=driver";
-- 不需要从Ray集群获取数据，需要将dataMode设置为model.
!python conf "dataMode=model";
```

## 使用Python代码处理数据，生成Excel

```python
!ray on simpleData '''

import io
import ray
from pyjava.api.mlsql import RayContext
import pandas as pd

ray_context = RayContext.connect(globals(),None)

rows = [item for item in RayContext.collect_from(ray_context.data_servers())]

df = pd.DataFrame(data=rows)
output = io.BytesIO()
writer = pd.ExcelWriter(output, engine='xlsxwriter')
df.to_excel(writer)
writer.save()
xlsx_data = output.getvalue()
context.build_result([{"file":xlsx_data}])

''' named excel_data;
```

简单解释下。

```python
ray_context = RayContext.connect(globals(),None)
```

获取ray_context,第二个参数为None表示不适用Ray集群。

```python
rows = [item for item in RayContext.collect_from(ray_context.data_servers())]

df = pd.DataFrame(data=rows)
```

获取表数据，并且将其转化为Pandas DataFrame.

剩下的代码是具体将数据转化为excel的代码，得到一个二进制数组。使用

```python
context.build_result([{"file":xlsx_data}])
```

输出到 excel_data 表中。

Pandas对Excel更多操作参考这里[working_with_pandas](https://xlsxwriter.readthedocs.io/working_with_pandas.html)

## 保存文件

最后，我们使用`!saveFile` 将excel保存起来.

```shell
!saveFile _ -i excel_data -o /tmp/wow.xlsx;
```

第一个参数  `_` 表示这里会使用命令行模式解析。 `-i` 表示输入的表， `-o` 表示输出路径。

## 附录

完整代码如下：

```sql
load delta.`public.simpleData` as simpleData;

-- !pyInclude "excel.save.py" named save_excel;

-- ray 配置
!python env "PYTHON_ENV=source activate dev";
!python conf "schema=st(field(file,binary))";
!python conf "runIn=driver";
!python conf "dataMode=model";

!ray on simpleData '''

import io
import ray
from pyjava.api.mlsql import RayContext
import pandas as pd

ray_context = RayContext.connect(globals(),None)

rows = [item for item in RayContext.collect_from(ray_context.data_servers())]

df = pd.DataFrame(data=rows)
output = io.BytesIO()
writer = pd.ExcelWriter(output, engine='xlsxwriter')
df.to_excel(writer)
writer.save()
xlsx_data = output.getvalue()
context.build_result([{"file":xlsx_data}])

''' named excel_data;

!saveFile _ -i excel_data -o /tmp/wow.xlsx;


```





