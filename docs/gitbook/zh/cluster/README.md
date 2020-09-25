## 管理多个MLSQL实例

之前的章节里，我们提到目前MLSQL有三个比较重要的组件：

1. MLSQL-Instance
2. MLSQL-Cluster
3. MLSQL-Console

MLSQL-Instance 就是一个MLSQL实例，但是不管什么原因，我们总是需要启动多个MLSQL实例的，这个时候我们就需要有
能够管理的和代理转发的工具，这个就是MLSQL-Cluster责任。MLSQL-Console则负责脚本管理，MLSQL-Cluster配置管理等，多用户等。

## 部署MLSQL-Cluster

### 下载
首先下载[MLSQL-Cluster发行包](http://download.mlsql.tech/mlsql_cluster-1.2.0-SNAPSHOT/),解压后目录结构如下：

```
-rw-r--r--   1 allwefantasy  wheel   957 Jan 24 10:01 application.yml
-rw-r--r--   1 allwefantasy  wheel  1032 Jan 24 10:01 db.sql
drwxr-xr-x   3 allwefantasy  wheel   102 Jan 24 10:01 libs/
-rw-r--r--   1 allwefantasy  wheel   234 Jan 24 10:01 logging.yml
-rwxr--r--   1 allwefantasy  wheel   160 Jan 24 10:01 start.sh*
```

### 初始化数据库

打开application.yml文件，找到如下部分

```
  1 mode:
  2   production
  3 
  4 production:
  5   datasources:
  6     mysql:
  7       host: 127.0.0.1
  8       port: 3306
  9       database: streamingpro_cluster
 10       username: root
 11       password: mlsql
 12       disable: false
 13     mongodb:
 14       disable: true
 15     redis:
 16       disable: true

```

我们根据需要修改host,port,username,password字段。db的话假设你没有修改，依然为streamingpro_cluster。

接着打开db.sql,然后将这些语句放到db中执行。

### 启动
运行 start.sh 脚本，启动内容如下：

```
(PyMLSQL) [w@me mlsql_cluster-1.2.0-SNAPSHOT]$ ./start.sh 
[2019-01-24 13:57:17,007][INFO ][com.alibaba.druid.pool.DruidDataSource] {dataSource-1} inited
[2019-01-24 13:57:17,017][INFO ][bootstrap.loader.impl    ] scan service package => null
[2019-01-24 13:57:17,018][INFO ][bootstrap.loader.impl    ] load service in ServiceFramwork.serviceModules =>0
[2019-01-24 13:57:17,018][INFO ][bootstrap.loader.impl    ] total load service  =>10
[2019-01-24 13:57:17,468][INFO ][org.eclipse.jetty.util.log] Logging initialized @2189ms
[2019-01-24 13:57:17,689][INFO ][bootstrap.loader.impl    ] controller load :    tech.mlsql.cluster.controller.BackendController
[2019-01-24 13:57:17,690][INFO ][bootstrap.loader.impl    ] controller load :    tech.mlsql.cluster.controller.EcsResourceController
[2019-01-24 13:57:17,691][INFO ][bootstrap.loader.impl    ] controller load :    tech.mlsql.cluster.controller.MLSQLProxyController
[2019-01-24 13:57:17,696][ERROR][bootstrap.loader.impl    ] load default controller error:java.lang.ClassNotFoundException: 
[2019-01-24 13:57:17,845][INFO ][org.eclipse.jetty.server.Server] jetty-9.2.z-SNAPSHOT
[2019-01-24 13:57:17,887][INFO ][org.eclipse.jetty.server.ServerConnector] Started ServerConnector@6523e61f{HTTP/1.1}{0.0.0.0:8080}
[2019-01-24 13:57:17,887][INFO ][org.eclipse.jetty.server.Server] Started @2610ms

```

可以看到监听的端口为8080。

## 部署MLSQL-Console

### 下载
下载 [MLSQL-Console下载地址](http://download.mlsql.tech/mlsql_api-1.2.0-SNAPSHOT/)，然后解压，目录结构如下：

```
[w@me mlsql_api-1.2.0-SNAPSHOT]$ ll
total 24
-rw-r--r--   1 allwefantasy  wheel  2687 Jan 24 10:01 application.yml
-rw-r--r--   1 allwefantasy  wheel  1432 Jan 24 10:01 db.sql
drwxr-xr-x   3 allwefantasy  wheel   102 Jan 24 10:01 libs/
-rwxr--r--   1 allwefantasy  wheel   238 Jan 24 10:01 start.sh*
```

### 配置数据库
然后打开application.yml，配置如下部分：

```
  1 #mode
  2 mode:
  3   development
  4 #mode=production
  5 
  6 ###############datasource config##################
  7 #mysql,mongodb,redis等数据源配置方式
  8 development:
  9   datasources:
 10     mysql:
 11       host: 127.0.0.1
 12       port: 3306
 13       database: mlsql_console
 14       username: root
 15       password: mlsql
 16       disable: false

```

一样，根据需要修改数据库配置信息。接着通过db.sql进行表创建。

### 设置环境变量

```
# 我们前面配置的MLSQL-Cluster地址和端口
export MLSQL_CLUSTER_URL="内网地址:8080"
# 方便其他系统能够连接到MLSQL-Console
export  MY_URL="内网地址:9002"
```

### 运行

设置完环境变脸后，就可以启动了，同样是运行start.sh脚本，输出如下：

```
^C[w@me mlsql_api-1.2.0-SNAPSHOT]$ ./start.sh 
[2019-01-24 14:16:23,189][INFO ][com.alibaba.druid.pool.DruidDataSource] {dataSource-1} inited
[2019-01-24 14:16:23,196][INFO ][bootstrap.loader.impl    ] scan service package => null
[2019-01-24 14:16:23,197][INFO ][bootstrap.loader.impl    ] load service in ServiceFramwork.serviceModules =>0
[2019-01-24 14:16:23,197][INFO ][bootstrap.loader.impl    ] total load service  =>10
[2019-01-24 14:16:23,557][INFO ][org.eclipse.jetty.util.log] Logging initialized @1626ms
[2019-01-24 14:16:23,738][INFO ][bootstrap.loader.impl    ] controller load :    tech.mlsql.api.controller.CloudController
[2019-01-24 14:16:23,739][INFO ][bootstrap.loader.impl    ] controller load :    tech.mlsql.api.controller.ClusterProxyController
[2019-01-24 14:16:23,740][INFO ][bootstrap.loader.impl    ] controller load :    tech.mlsql.api.controller.UserController
[2019-01-24 14:16:23,741][INFO ][bootstrap.loader.impl    ] controller load :    tech.mlsql.api.controller.UserScriptFileController
[2019-01-24 14:16:23,746][ERROR][bootstrap.loader.impl    ] load default controller error:java.lang.ClassNotFoundException: 
[2019-01-24 14:16:23,939][INFO ][org.eclipse.jetty.server.Server] jetty-9.2.z-SNAPSHOT
[2019-01-24 14:16:23,979][INFO ][org.eclipse.jetty.server.ServerConnector] Started ServerConnector@2a317ed2{HTTP/1.1}{0.0.0.0:9002}
[2019-01-24 14:16:23,979][INFO ][org.eclipse.jetty.server.Server] Started @2050ms

```
默认监听端口9002

## MSLQL-Console使用说明

访问http://[内网地址]:9002，这个时候会有如下界面：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-357a608cef670646.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


点击右上角Register进行注册。

> 第一个用户会是管理员用户


接着进入主界面：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-ee11effee23542ba.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

默认运行任何指令都会失败，因为还没有配置后端MLSQL执行引擎实例。我们点击 Cluster 标签页：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-b8fb001dba3b2ea9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

通过Add Backend进入添加后端实例的页面：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-80aad8037095d1a4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

url部分是你的MLSQL实例地址，最好填写内网地址。点击添加后，就可以在List Backend页面看到列表了：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-14087d1415882110.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

这里看到， backend3是被激活的。

> 我们并不能激活某个实例，我们只能激活一个或者多个标签，具有相应标签的实例则会被自动启用。设置需要使用的标签，在 

Set Console Backend里：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-fd05a47d08880c19.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

我选择使用具有所有jack标签的实例。

现在你可以回到Console里去执行操作了：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-317a4b6928c8c6c1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

上面的有两个进度条：

一个表示当前MLSQL实例总共资源数为8cpu, 你用了1个，总共用了1个。
第二个表示，当前正在执行的job有5个任务，有0个完成，一个正在运行。Job数自身是未知的，因为脚本是解释执行的。这个只是反映的当前的job(被解释的指令)的任务情况。

如果我想取消当前的任务，该怎么办呢？刷新下当前页面，然后使用如下指令查看所有任务：


![image.png](https://upload-images.jianshu.io/upload_images/1063603-99ed03c4f05f2fa9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

任务列表如下：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-8c77df730e4abf14.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

我们通过该列表可以看到我们的任务名或者组名，我们以groupId为例，对应的是10,通过如下指令可以杀死该任务：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-73d35abdabc8261b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

