# 常见问题集锦



* MLSQL 架构图有么？
* 数据源有详细参数配置文档么？...
* 后台是怎么区分batch还是streaming的？比如我load kafka，同时又load hbase,mysql或者es，这种情况下底层对应的作业时 streaming的还是batch的，逻辑都是在window范围内执行的吗
* engine本身是个spark的app，里面提供的restful服务，那么这个服务可以是高可用的吗？
*  权限管理  比如A,B,C 3张表  允许用户查询A表，BC两张不可查 这种授权是自己写client实现吗？
* 如果udf的代码比较多，同时又import了很多第三方的jar包，怎么处理？



> MLSQL 架构图有么？

![image.png](https://upload-images.jianshu.io/upload_images/1063603-342e726ed4b80766.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

MLSQL Console 包含了几个组件，分别是
1. script center (脚本管理体系)
2. auth center (权限体系)
3. web (交互探索使用)
4. api (供接口调用)

其中1,2 会被后端的MLSQL Engine回调，从而实现一系列功能。

MLSQL Cluster 主要是为了方便多租户，多Engine实例管理，你可以理解为一个路由功能（虽然不局限于此）。在MLSQL Console中，默认每个Engine实例都会被打上一组team_role标签,这样就可以给任何一个role配置一个或者多个MLSQL Engine,从而实现例如负载均衡等功能。

MLSQL Engine 底层基于Spark Engine,是一个典型的master-slave结构，在原生的Spark SQL之上，我们提供了MLSQL语言，从而更好的满足复杂交互诉求，比如批处理脚本，机器学习，邮件发送等等。

大家可以一键体验上面的所有功能:[MLSQL生态一键体验](https://www.jianshu.com/p/5f375cf9b464)

> 数据源有详细参数配置文档么？比如kafka，我可以理解成kafka consumer的配置都可以写到option里面吗

MLSQL大部分数据源集成的是第三方实现。比如excel的支持得益于spark-excel项目。同样，Kafka的配置参数和Spark 对Kafka的需求配置是一样的，JDBC则也是标准的Spark文档中描述的那样。不过大部分人使用时，不会使用所有参数，
MLSQL也提供了两种方式展示可选参数：

1. 使用MLSQL Console, Console支持参数自动补全

![image.png](https://upload-images.jianshu.io/upload_images/1063603-5e54d87583e23f73.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

MLSQL Console 实现了数据源和参数联动。不过目前只有部分数据源支持，我们会尽快覆盖所有数据源。

2. 使用帮助语句。

查看所有数据源：

```sql
load _mlsql_.`datasources` as output;
```

查看具体某个数据源的可选参数：

```
load _mlsql_.`datasources/params/excel` as output;
```

![image.png](https://upload-images.jianshu.io/upload_images/1063603-07ad18f7310daa71.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

除了数据源，其他所有MLSQL特有模块，也都是支持前面两种方式的。比如，我想查看BigDL模块的示例：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-3a8c7ea31a031690.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

接着就可以看到：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-9b563f60c7298f6e.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

或者通过命令查看所有可选参数：

```sql
load modelParams.`BigDLClassifyExt` as output;
```

> 后台是怎么区分batch还是streaming的？比如我load kafka，同时又load hbase,mysql或者es，这种情况下底层对应的作业时streaming的还是batch的，逻辑都是在window范围内执行的吗

后台是根据 `set streamName="streamExample";`因为流式计算我们需要用户定义一个唯一的名字。MLSQL底层是使用spark structured streaming,所以structured streaming存在的限制，MLSQL都存在。structured streaming支持对静态数据的Join。如果您需要深入，请多了解structured streaming。

> engine本身是个spark的app，里面提供的restful服务，那么这个服务可以是高可用的吗？

Engine自身无法保证高可用，但是你可以通过如下两种方式的一种保持其高可用：

1. 第一是部署环境比如在yarn-cluster模式下，Engine支持将自己注册到ZK中，而Yarn又能保证driver挂掉后自动找一个其他节点启动，但在yarn-client模式则不行。 

2. 第二个是，通过MLSQL-Cluster来完成。MLSQL-Cluster 现在实现了多策略的负载均衡，以及多集群的管理。通过负载均衡，也可以保证Engine的高可用，比如后端部署三个Engine,任意down掉两个，都不影响。

> 权限管理  比如A,B,C 3张表  允许用户查询A表，BC两张不可查
这种授权是自己写client实现吗？

MLSQL Console已经实现了一个。你也可以参考它实现自己的。MLSQL 架构足够灵活可以让你做很多定制扩展。为了实现A,B,C,三张表的授权，大致有如下几个流程：

*** 配置Team ***

1. 创建一个Team 比如： TestTeam(有则忽略)
2. 邀请用户到Team
3. 给Team添加Role
4. 给Team添加表资源

*** 配置Role***
1. 给Role添加一个Bakend(有则忽略)
4. 将需要的表权限授Role
6. 将用户角色设置为需要的Role

*** 用户自身配置 ***

用户登陆后，可能在多个team,多个role里，所以他需要配置下自己默认运行的Backend（MLSQL Engine）是什么。


如果是在MLSQL Console 里操作如上流程：

1. 创建TestTeam

![image.png](https://upload-images.jianshu.io/upload_images/1063603-80aa2d94232a8879.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

2. 创建Role 

![image.png](https://upload-images.jianshu.io/upload_images/1063603-2df4504e8dae9b97.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

2.1 给Role指定一个Backend:

![image.png](https://upload-images.jianshu.io/upload_images/1063603-1a83f509be3ac284.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


3. 邀请用户到特定Role

![image.png](https://upload-images.jianshu.io/upload_images/1063603-607f11b1460032a3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

4. 创建A,B,C三张表

![image.png](https://upload-images.jianshu.io/upload_images/1063603-c49d2c6e92fed1f0.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

5. 给Role添加表权限

![image.png](https://upload-images.jianshu.io/upload_images/1063603-78c43904729a66ed.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

6. 被授权用户将自己默认的Backend设置为新的：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-2ae9fe1376cf03ab.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

现在被授权用户可以正常使用了。

> 如果udf的代码比较多，同时又import了很多第三方的jar包，怎么处理？

MLSQL自身的项目已经非常庞大，理论上你UDF要的库，都有了。如果没有，你需要用--jars带上。某些场景可能还需要配置driver/executor classpath,不过最简单的方案是把你的jar包放到spark 发型包里。









