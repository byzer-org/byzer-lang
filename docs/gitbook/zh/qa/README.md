# 常见问题集锦

> MLSQL 架构图有么？

MLSQL 架构如下：

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



