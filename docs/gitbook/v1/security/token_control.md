#接口访问Token设置

MLSQL Engine 对外提供HTTP服务默认是没有权限验证的，原因是我们假设他是在一个内网安全环境，只有特定应用才能访问该服务。MLSQL Engine从2.0.0版本开始对接口访问提供了Token验证。

## 设置启动Token

在spark-submit 命令行里，添加

```
--conf spark.mlsql.auth.access_token=your-token-string
```

最后如果用户要访问Engine接口，必须在请求参数里带上 your-token-string才能访问。
对于MLSQL Console,则需要在建立Engine的时候，填写 token:

![](http://docs.mlsql.tech/upload_images/ff318bc1-e126-465f-ae57-8753712816f0.png)
