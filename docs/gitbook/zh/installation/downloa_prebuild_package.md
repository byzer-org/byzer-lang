#下载预编译的安装包

MLSQL Stack 提供了对各个版本的预编译版本。 用户可以在[下载站点](http://download.mlsql.tech)获得需要的版本。

> 我们推荐大家安装最新版本 1.6.0-SNAPSHOT


## 下载目录结构介绍

下图是一个典型的下载界面。

![](http://docs.mlsql.tech/upload_images/WX20190818-110800@2x.png)

以-SNAPSHOT结尾的目录，表示对应的版本是日常更新的。比如有新的功能，我们可能会覆盖更新对应版本的包。而没有
以-SNAPSHOT结尾的目录，则表示是稳定版本。以图片为例，最新的1.4.0稳定版还没有发布，但是我们在不断的更新1.4.0-SNAPSHOT。

进入1.4.0-SNAPSHOT(对应的稳定版本目录结构是会是一样的)，我们会发现有如下几个文件和目录：

![](http://docs.mlsql.tech/upload_images/WX20190818-111503@2x.png)

从图中实例，我们发现有有四个文件，分别是cluster,console,和engine。 其中engine根据底层的Spark版本，又区分为2.3/2.4两个版本。
所以，大家在下载engine的时候，需要根据自己要运行的Spark版本选择合适的版本。通常，我们建议大家下载基于最新的spark版本的发行包，
因为MLSQL新功能特性都会优先体现在最近的Spark版本之上。

## 启动和配置 MLSQL Engine

选择合适的MLSQL Engine 版本下载后，解压，目录大致如下：

![](http://docs.mlsql.tech/upload_images/WX20190818-120157@2x.png)

运行如下脚本即可在本机运行

```
./start-default.sh
```

如果无法运行，尝试给该脚本赋予可执行权限

```
chmod u+x start-default.sh
```

启动后，你可以访问本地的9003端口，可看到一个非常简易的页面。

如果需要尝试将其运行于其他环境，比如Yarn， 你可以打开start-local.sh，将

```
--master local[*]
```
替换成

```
--master yarn-client

```

即可。当然，这需要你已经将hdfs,yarn等相关的配置文件放到SPARK_HOME/conf里。


## 启动和配置 MLSQL Cluster (可选)

同样，下载解压后，目录结构如下：

![](http://docs.mlsql.tech/upload_images/WX20190818-130445@2x.png)


因为MLSQL Cluster 依赖MySQL,所以

1. 用户需要创建一个数据库streamingpro_cluster(你也可以取一个你喜欢的名字)
2. 将根目录下的db.sql导入
3. 修改application.docker.yml,将"MYSQL_HOST" 占位符替换成MySQL地址，同时如果有必要，修改对应的数据库，用户名称，密码等。 
4. 现在可以调用`./start-default.sh` 启动了

启动后，默认监听的是8080端口。你也可以修改，具体配置位于修改application.docker.yml. MLSQL Cluster 并没有Web页面，它主要是衔接
MLSQL Console 和Engine的。

> MySQL 5.7经过测试，如发生java.math.BigInteger can not cast to java.lang.Long等错误，可尝试降级MySQL的版本。

## 启动和配置 MLSQL Console

同样，从官方站点下载后，解压，得到目录结构如下：

![](http://docs.mlsql.tech/upload_images/WX20190818-160123@2x.png)

MLSQL Console也需要依赖MySQL,所以，同cluster一样，你需要经过相同的几个配置步骤才能让它正常工作。

1. 用户需要创建一个数据库mlsql_console(你也可以取一个你喜欢的名字)
2. 将根目录下的db.sql导入
3. 修改application.docker.yml,将"MYSQL_HOST" 占位符替换成MySQL地址，同时如果有必要，修改对应的数据库，用户名称，密码等。 
4. 现在可以调用`./start-default.sh` 启动了

启动后，默认监听的是9002端口。你也可以修改，具体配置修改位于application.docker.yml. MLSQL Console提供了一个较为完善的界面。
用户可自助注册和登录。 

打开start-default.sh,其展示的内容类似下面：

```
export MLSQL_CONSOLE_JAR="mlsql-api-console-1.4.0-SNAPSHOT.jar"

# 如果你没有安装cluster,那么这个请注释掉这句，并且开启下面那一句
export MLSQL_CLUSTER_URL=http://127.0.0.1:8080

# 如果你没有安装cluster,请开启这一句
# export MLSQL_ENGINE_URL=http://127.0.0.1:9003

export MY_URL=http://127.0.0.1:9002
export USER_HOME=/home/users
export ENABLE_AUTH_CENTER=false
export MLSQL_CONSOLE_CONFIG_FILE=application.docker.yml

SELF=$(cd $(dirname $0) && pwd)
cd $SELF

./start.sh
```

如果你没有安装cluster,修改start.sh为如下内容：

```
#!/usr/bin/env bash

java -cp .:${MLSQL_CONSOLE_JAR} tech.mlsql.MLSQLConsole \
-mlsql_engine_url ${MLSQL_ENGINE_URL} \
-my_url ${MY_URL} \
-user_home ${USER_HOME} \
-enable_auth_center ${ENABLE_AUTH_CENTER:-false} \
-config ${MLSQL_CONSOLE_CONFIG_FILE}
```

前面export 部分本质上都是配置。其中有几个配置值得大家注意。第一个是MLSQL_CLUSTER_URL，他表示MLSQL Cluster的地址。前面我们已经
在本地启动了MLSQL Cluster,所以对一个的地址是 `http://127.0.0.1:8080`。MY_URL 则是表示MLSQL Console自己的地址。MLSQL CLuster 和
MLSQL Engine需要反向连接MLSQL Console,这里其实是告诉他们两MLSQL Console的地址是什么。MLSQL_ENGINE_URL则是engine的直接地址，如果你没有
配置cluster，则会显示上面的效果。

USER_HOME 是为了契合MLSQL Stack里一个比较特殊的概念。 MLSQL Stack 设计的时候是面向多租户的，这意味着，每个用户有自己的执行环境，这也包括
主目录，正如其他的系统譬如Linxu一样。通常如果你是local模式部署MLSQL Engine,那么主目录是MLSQL Engine所在机器的目录。如果你是分布式部署，那么
一般而言是HDFS。 USER_HOME 是所有使用MLSQL Console用户的主目录的根目录。比如，假设用户william,那么他对应的主目录为

```
/home/users/william
```

如果你使用

```sql
!hdfs -ls /;
```

你看到的也是/home/users/william这个目录下的目录，并不会看到其他人的目录。

ENABLE_AUTH_CENTER 参数指是否开启权限校验。默认是不开启。

现在，MLSQL Console也启动了，我们需要注册和登录进去。

## 开启Cluster后的额外配置

如果你安装了cluster,则需要进行一些额外配置，来确保我们可用。其配置和[使用Docker安装体验](https://docs.mlsql.tech/zh/installation/docker.html)里描述
的大部分一致。唯一需要变更的地方是：

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095834.png)

将mlsql-server:9003替换成实际的MLSQL Engine地址。 

 
                 
