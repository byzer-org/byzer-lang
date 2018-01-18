## 执行一个批处理任务

如果我希望把数据库的某个表的数据同步成parquet文件，这个通过StreamingPro是一件极其简单的事情，我们写一个简单的配置文件：

```
{
  "mysql-table-export-to-parquet": {
    "desc": "把mysql表同步成parquet文件",
    "strategy": "spark",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
          "name": "batch.sources",
          "params": [
            {
               url:"jdbc:mysql://localhost/test?user=fred&password=secret",
              "dbtable":"table1",
              "driver":"com.mysql...",
              "path": "-",
              "format": "jdbc",
              "outputTable": "test",

            },
            {
              "path": "/user/data/a.json",
              "format": "json",
              "outputTable": "test2",
              "header": "true"
            }
          ]
       },
      {
        "name": "batch.sql",
        "params": [
          {
            "sql": "select test.* from test left join test2 on test.id=test2.id2",
            "outputTableName": "tempTable1"
          }
        ]
      },
     {
      "name": "batch.sql",
      "params": [
        {
          "sql": "select test.* from tempTable1 left join test2 on tempTable1.id=test2.id2",
          "outputTableName": "tempTable2"
        }
      ]
        },
      {
        "name": "batch.outputs",
        "params": [
          {
            "name":"jack",
            "format": "parquet",
            "path": "/tmp/parquet1",
            "inputTableName": "tempTable2",
            "mode":"Overwrite"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```


这个例子显示了如何配置多个数据源，并且sql可以如何进行交互，最后如何进行输出。batch.sources,batch.sql,batch.outputs完全是
以表来进行连接的，我们可以使用很多sql，通过生成中间表的形式来较为简单的完成一个任务。

batch.sql 目前只能配置一条sql语句，但是一个配置文件可以写很多batch.sql。batch.sql之间可以互相依赖，并且有顺序之分。每个batch.sql
都需要指定一个输出表，除非你执行的ddl语句。

在这个例子中，batch.sql 其实是可有可无的，如果只是单纯同步数据，你可以只保留batch.sources/batch.outputs