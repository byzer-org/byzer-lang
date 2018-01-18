## 对Flink的支持

进入flink安装目录运行如下命令：

```
./bin/start-local.sh
```

之后写一个flink.json文件：


```
{
  "flink-example": {
    "desc": "测试",
    "strategy": "flink",
    "algorithm": [],
    "ref": [],
    "compositor": [
      {
        "name": "flink.sources",
        "params": [
          {
            "format": "socket",
            "port": "9000",
            "outputTable": "test"
          }
        ]
      },
      {
        "name": "flink.sql",
        "params": [
          {
            "sql": "select * from test",
            "outputTableName": "finalOutputTable"
          }
        ]
      },
      {
        "name": "flink.outputs",
        "params": [
          {
            "name":"jack",
            "format": "console",
            "inputTableName": "finalOutputTable"
          }
        ]
      }
    ],
    "configParams": {
    }
  }
}
```
目前source 只支持 kafka/socket ，Sink则只支持console和csv。准备好这个文件你就可以提交任务了：

./bin/flink run  -c streaming.core.StreamingApp \ /Users/allwefantasy/streamingpro/streamingpro.flink-0.4.14-SNAPSHOT-online-1.2.0.jar
-streaming.name god \
-streaming.platform flink_streaming \
-streaming.job.file.path file:///Users/allwefantasy/streamingpro/flink.json

然后皆可以了。

你也可以到localhost:8081 页面上提交你的任务。