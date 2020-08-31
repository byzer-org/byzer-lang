# 结合Python读取Excel

MLSQL 2.0.1-SNAPSHOT/2.0.1 以及以上版本可用

> MLSQL 提供了 [excel插件](https://github.com/allwefantasy/mlsql-plugins/tree/master/mlsql-excel)，可以让用户读写Excel。更多详情参考[插件](http://docs.mlsql.tech/mlsql-engine/plugin/)


Python对Excel的支持也很不错，而MLSQL支持Python,所以自然能够在MLSQL操作和使用Excel数据。这样我们可以使用MLSQL将大规模的数据变小，然后保存成Excel后用户下载下来做进一步处理。

## 读取excel文件

```shel
load binaryFile.`/tmp/wow.xlsx` as excel_table;
```

我们也可以读取一个目录下所有的excel文件，只要把路径写到目录即可。


## Ray配置

```
-- python处理完后的数据的schema
!python conf "schema=st(field(file,binary))";
-- Python跑在driver端，
!python conf "runIn=driver";
-- 不需要从Ray集群获取数据，需要将dataMode设置为model.
!python conf "dataMode=model";
```

## 使用Python解析Excel

```python
!ray on excel_table '''

import io
import ray
from pyjava.api.mlsql import RayContext
import pandas as pd
ray_context = RayContext.connect(globals(),None)

excel_file_binary_list = [item for item in RayContext.collect_from(ray_context.data_servers())]

df = pd.read_excel(io.BytesIO(excel_file_binary_list[0]["content"]))

context.build_result([row for row in df.to_dict('records')])

''' named excel_data;
```

简单解释下。

```python
ray_context = RayContext.connect(globals(),None)
```

获取ray_context,第二个参数为None表示不适用Ray集群。

```python
excel_file_binary_list = [item for item in RayContext.collect_from(ray_context.data_servers())]
```

获取所有文件内容。

```python
df = pd.read_excel(io.BytesIO(excel_file_binary_list[0]["content"]))
```

我们这里只有一条记录，对应的二进制内容字段是content.

```python
context.build_result([row for row in df.to_dict('records')])
```

将结果展开成行构建返回。

![](http://docs.mlsql.tech/upload_images/490df52e-47f6-4dbf-8c8d-534a1dd679f9.png)


## 附录

下面是完整代码：

```python
load binaryFile.`/tmp/wow.xlsx` as excel_table;
!pyInclude excel.read.py named read_excel;

-- ray 配置
!python env "PYTHON_ENV=source activate dev";
!python conf "schema=st(field(a,string),field(b,string))";
!python conf "runIn=driver";
!python conf "dataMode=model";

!ray on excel_table '''

import io
import ray
from pyjava.api.mlsql import RayContext
import pandas as pd


ray_context = RayContext.connect(globals(),None)

excel_file_binary_list = [item for item in RayContext.collect_from(ray_context.data_servers())]

df = pd.read_excel(io.BytesIO(excel_file_binary_list[0]["content"]))

context.build_result([row for row in df.to_dict('records')])

''' named excel_data;



```





