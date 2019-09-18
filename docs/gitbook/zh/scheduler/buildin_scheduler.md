# 内置调度

> 该功能需要1.5.0版本及以上

MLSQL 内置了一个调度系统，我们通常强力推荐用户设置一个单独的MLSQL Engine作为调度系统。启用的方式也很简答，你只需要
启动的时候设置如下几个参数：

1. -streaming.datalake.path
2. -streaming.workAs.schedulerService true
3. -streaming.workAs.schedulerService.consoleUrl console 地址
4. -streaming.workAs.schedulerService.consoleToken console 秘钥

console的秘钥需要在启动console的时候进行设置。也就是在配置文件application.yml文件里设置。 在对应的application.yml文件
按如下方式添加秘钥：

```
auth_secret: "mlsql"
```

当你启动好后，此时MLSQL Engine就是一个调度系统了。 那么如何在Console中使用呢？我们还需要在Console中做几个配置。

登录Console,并且进入Team/Team 中，添加一个新角色：

![](http://docs.mlsql.tech/upload_images/WX20190914-160046@2x.png)

接着进入Cluter标签中，在这里添加你刚启动的Engine,

![](http://docs.mlsql.tech/upload_images/WX20190914-160159@2x.png)

最后一个框选择刚刚创建的Role,现在，可以设置你的默认scheduler服务了 :

![](http://docs.mlsql.tech/upload_images/WX20190914-160242@2x.png)


进入console,新建一个scheduler-test.nb文件：

![](http://docs.mlsql.tech/upload_images/WX20190914-160337@2x.png)

上面就是一些常见操作指令。比如

```sql
!scheduler "bigbox.main.mlsql" with "*/3 * * * * ";
```

表示要每三分钟执行bigbox下的main.mlsql脚本。

接着你也可以设置对该该脚本的依赖：

```sql
!scheduler "demo.dash2.mlsql" depends on "bigbox.main.mlsql"; 
```

