# 自助下载源码编译打包

因为MLSQL Stack包含三个部件，所以为了运行起一个完整的Stack,您需要编译三个项目

## MLSQL Engine

1. clone项目

```
git clone https://github.com/allwefantasy/streamingpro.git
```

2. 编译

```
cd streamingpro
export MLSQL_SPARK_VERSION=2.4
./dev/package.sh
```

你会在如下目录发现一个jar包(注意，需要把x.x.x替换为实际的版本,后续相同)：

```
streamingpro-mlsql/target/streamingpro-mlsql-spark_2.4_2.11-x.x.x-SNAPSHOT.jar
```

大概280多M。 接着手动创建一个发行包：

```
export VERSION=x.x.x-SNAPSHOT
mkdir -p /tmp/mlsql-server/libs
cp  streamingpro-mlsql/target/streamingpro-mlsql-spark_2.4_2.11-${VERSION}.jar /tmp/mlsql-server/libs
cp dev/start-local.sh /tmp/mlsql-server
cd /tmp/mlsql-server

export SPARK_HOME=~/Softwares/spark-2.4.0-bin-hadoop2.7
./start-local.sh
```

这个时候可以访问 127.0.0.1:9003了；


## MLSQL Cluster

cluster 和engine在同一个项目里。

```
cd streamingpro
export MLSQL_CLUSTER_VERSION=${MLSQL_CLUSTER_VERSION:-x.x.x-SNAPSHOT}
mvn -DskipTests -Pcluster-shade -am -pl streamingpro-cluster clean package
cd streamingpro-cluster/
```

编译完成后，在target 目录会有如下文件：

```
export VERSION=-2.4_2.11-x.x.x-SNAPSHOT
target/streamingpro-cluster-${VERSION}.jar
```


创建MySQL数据库,根据 `src/main/resources/db.sql `创建对应的库表。数据库名字为mlsql_cluster.

> MySQL 5.7经过测试，如发生java.math.BigInteger can not cast to java.lang.Long等错误，可尝试降级MySQL的版本。

现在，创建一个发行版：

```
# make sure you are in  streamingpro-cluster
#cd streamingpro-cluster/
export VERSION=-2.4_2.11-x.x.x-SNAPSHOT
mkdir -p /tmp/mlsql-cluster/
cp  target/streamingpro-cluster-${VERSION}.jar /tmp/mlsql-cluster/
cp dev/mlsql-cluster-docker/start.sh  /tmp/mlsql-cluster

##修改application.docker.yml 数据库地址和密码
cp dev/mlsql-cluster-docker/application.docker.yml  /tmp/mlsql-cluster
cd  /tmp/mlsql-cluster

export MLSQL_CLUSTER_CONFIG_FILE=application.docker.yml
export MLSQL_CLUSTER_JAR=streamingpro-cluster-${VERSION}.jar
./start.sh
```

这个时候查看127.0.0.1:8080/backend/active是否可以访问，如果成功则会返回{}。

## MLSQL Console

1. 下载项目

```
git clone  git@github.com:allwefantasy/mlsql-api-console.git
```

2. 编译打包

```
mvn clean package -Pshade
```

这个时候在target目录有个文件：

```
export VERSION=x.x.x-SNAPSHOT
target/mlsql-api-console-${VERSION}.jar
```

3. 创建MySQL数据库,根据 `src/main/resources/db.sql `创建对应的库表。数据库名字为mlsql_console.

4. 创建发行包

```
export VERSION=x.x.x-SNAPSHOT
cd mlsql-api-console
mkdir -p /tmp/mlsql-console/
cp target/mlsql-api-console-${VERSION}.jar /tmp/mlsql-console/
cp  dev/docker/start.sh  /tmp/mlsql-console/
## 修改配置文件数据库地址，账号和密码
cp dev/docker/application.docker.yml   /tmp/mlsql-console/

cd /tmp/mlsql-console/

export MLSQL_CONSOLE_JAR="mlsql-api-console-${VERSION}.jar"
export MLSQL_CLUSTER_URL=http://127.0.0.1:8080
export MY_URL=http://127.0.0.1:9002
## mac下请换目录
export USER_HOME=/home/users 
## 是否开启权限控制
export ENABLE_AUTH_CENTER=false 
export MLSQL_CONSOLE_CONFIG_FILE=application.docker.yml
./start.sh
```

现在可以访问 127.0.0.1:9002了。

## 我还想DIY前端咋办？

1. clone 项目

```
git clone  git@github.com:allwefantasy/mlsql-web-console.git
```

2. 安装和编译

```
npm install
npm run build
```

3. 拷贝前端文件到MLSQL-Console里

```
rm -rf ${MLSQL_CONSOLE_HOME}/src/main/resources/streamingpro/assets/*
cp -r build/* ${MLSQL_CONSOLE_HOME}/src/main/resources/streamingpro/assets/*
```