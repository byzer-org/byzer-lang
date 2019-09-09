## Manage multiple MLSQL instances
   


In the previous chapters, we mentioned that the MLSQL system has three important components:

1. MLSQL-Instance
2. MLSQL-Cluster
3. MLSQL-Console

We must have started multiple MLSQL instances (MLSQL-Instance) for a variety of reasons. At this point, MLSQL-Cluster is used to manage, proxy and forward multiple instances. MLSQL-Console is responsible for script management, configuration management and multi-user management.
## Deploy MLSQL-Cluster

### Download
First download [MLSQL-Cluster release package](http://download.mlsql.tech/mlsql_cluster-1.2.0-SNAPSHOT/),After decompression, the directory structure is as follows:
                                                                                                         


```
-rw-r--r--   1 allwefantasy  wheel   957 Jan 24 10:01 application.yml
-rw-r--r--   1 allwefantasy  wheel  1032 Jan 24 10:01 db.sql
drwxr-xr-x   3 allwefantasy  wheel   102 Jan 24 10:01 libs/
-rw-r--r--   1 allwefantasy  wheel   234 Jan 24 10:01 logging.yml
-rwxr--r--   1 allwefantasy  wheel   160 Jan 24 10:01 start.sh*
```

### Initialize the database
    


Open the application.yml file and find the following sections


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

Modify the configuration information of host, port, username and password according to the actual situation.Then run the db.sql script in the database.



### start up
Run the start.sh script and logging is as follows:


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

From the running log, you can see that the listening port is 8080.


## Deploy MLSQL-Console

### Download
Download [MLSQL-Console download address](http://download.mlsql.tech/mlsql_api-1.2.0-SNAPSHOT/)，After decompression, the directory structure is as follows：

```
[w@me mlsql_api-1.2.0-SNAPSHOT]$ ll
total 24
-rw-r--r--   1 allwefantasy  wheel  2687 Jan 24 10:01 application.yml
-rw-r--r--   1 allwefantasy  wheel  1432 Jan 24 10:01 db.sql
drwxr-xr-x   3 allwefantasy  wheel   102 Jan 24 10:01 libs/
-rwxr--r--   1 allwefantasy  wheel   238 Jan 24 10:01 start.sh*
```

### Configure the database
    

The main configuration of the application.yml file is the following parts


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

Modify the configuration information of host, port, username and password according to the actual situation.Then run the db.sql script in the database.

### Setting environment variables

```
# Configure the address and port of the MLSQL-Cluster service in front.
export MLSQL_CLUSTER_URL="内网地址:8080"
# Convenient for other systems to connect to MLSQL-Console
export  MY_URL="your inside network:9002"
```

### Run

Run the start.sh script and log output is as follows


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
Default listening port 9002



## Instructions for the use of MSLQL-Console

Visit http://[inside address]: 9002, the interface is as follows:


![image.png](https://upload-images.jianshu.io/upload_images/1063603-357a608cef670646.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


Click "Register" in the upper right corner to register.


> The first user is administrator user by default.
  



Then enter the main interface:


![image.png](https://upload-images.jianshu.io/upload_images/1063603-ee11effee23542ba.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Any mlsql statement running at that time will fail because no instance of the backend MLSQL execution engine has yet been configured. We click on the Cluster tab:

![image.png](https://upload-images.jianshu.io/upload_images/1063603-b8fb001dba3b2ea9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Click "Add Backend" and add backend instance：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-80aad8037095d1a4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

The URL is the MLSQL instance address, it is better to fill in the inside address. After clicking "Add", you can see the list on the "List Backend" page:

![image.png](https://upload-images.jianshu.io/upload_images/1063603-14087d1415882110.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

As shown in the above example, backend 3 is in the active state.


> In fact, we indirectly activate instances by updating one or more tags. Instances with labels are automatically enabled
 

![image.png](https://upload-images.jianshu.io/upload_images/1063603-fd05a47d08880c19.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

I chose to execute mlsql statements using all instances of "jack" Tags



![image.png](https://upload-images.jianshu.io/upload_images/1063603-317a4b6928c8c6c1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

There are two progress bars above.
The first progress bar indicates that the current MLSQL instance has a total of 8 CPU resources, one of which is used.
The second progress bar indicates that a total of five tasks are being performed, of which 0 are completed and 1 is running.
Because scripts are executed interpretatively, the number of Jobs is usually unknown.

How to cancel the current running tasks? Refresh the current page and run the following instructions to view all tasks
                                        

![image.png](https://upload-images.jianshu.io/upload_images/1063603-99ed03c4f05f2fa9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Task list is as below:

![image.png](https://upload-images.jianshu.io/upload_images/1063603-8c77df730e4abf14.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Task or group names can be seen from this list.
You can kill the task by following instructions (take the groupId value of 10 as an example)


![image.png](https://upload-images.jianshu.io/upload_images/1063603-73d35abdabc8261b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

