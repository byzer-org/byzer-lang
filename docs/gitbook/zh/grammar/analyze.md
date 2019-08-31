# MLSQL语法解析接口

MLSQL Engine提供了语法解析模式。这主要可以帮助用户更好的UI体验：

1. 抽取set语法，从而实现表单功能
2. 抽取各个语句部分，从而在Web上实现模块化交互。

语法解析需要在调用MLSQL Engine接口时提供一个新的参数，executeMode,默认是query,
也就是执行查询操作，如果你设置为analyze,则会执行语法解析操作：

```sql
/run/script?executeMode=analyze
```

假设我们的MLSQL脚本如下：

```sql
set a="b"; load delta.`/tmp/delta/rate-3-table` where 
startingVersion="12"
and endingVersion="14"
as table1;

select __delta_version__, collect_list(key), from table1 group by __delta_version__,key 
as table2;
```

解析结果如下：


```json
[
    {
        "raw": "!delta history /tmp",
        "command": "delta",
        "parameters": [
            "history",
            "/tmp"
        ]
    },
    {
        "raw": "set a=\"b\"",
        "key": "a",
        "command": "\"b\"",
        "original_command": "\"b\"",
        "option": {},
        "mode": ""
    },
    {
        "raw": "load delta.`/tmp/delta/rate-3-table` where \nstartingVersion=\"12\"\nand endingVersion=\"14\"\nas table1",
        "format": "delta",
        "path": "`/tmp/delta/rate-3-table`",
        "option": {
            "startingVersion": "12",
            "endingVersion": "14"
        },
        "tableName": "table1"
    },
    {
        "raw": "select __delta_version__, collect_list(key), from table1 group by __delta_version__,key \nas table2",
        "sql": "select __delta_version__, collect_list(key), from table1 group by __delta_version__,key \n",
        "tableName": "table2"
    }
]

```