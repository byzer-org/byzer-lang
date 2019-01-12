# 多租户配置

多租户并不是在启动脚本中配置，而是在请求时通过http请求参数来配置。运行一个脚本的url为：

```shell
curl -XPOST `http://127.0.0.1:9003/run/script` -d '
sql=      脚本
owner=    登录用户
jobType=  script
jobName=  一般会使用uuid
timeout=  该次请求的超时时间
silence=  是否返回结果
sessionPerUser= true/fase 是否开启多租户。默认为false
async=    true/false 是否异步执行，默认false
callback= 当async 设置为true时，异步的回调地址
skipInclude= true/false 是否需要禁止脚本include 默认为false
skipAuth= 是否需要权限验证，默认为false 
'
```

和多租户相关的参数有：

```
sessionPerUser=true
```

请设置为true，这个时候就实现用户隔离了，每个用户都会创建一个session. 客户端需要自己通过接口
进行会话注销。

```
http://127.0.0.1:9003/run/script?owner=&sessionPerUser=true
```


