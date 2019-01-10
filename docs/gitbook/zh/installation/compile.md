# 自助编译打包

## MLSQL-Engine编译方式

```shell
# clone project
git clone https://github.com/allwefantasy/streamingpro .
cd streamingpro

# 通过该环境变量控制底层的spark版本，支持 2.2/2.3/2.4 默认2.3
export MLSQL_SPARK_VERSIOIN=2.3

## build  
./dev/package.sh
```

在maven == 3.3.9 环境测试编译打包没有问题。运行完成后，会有如下文件：

```
streamingpro-mlsql/target/streamingpro-mlsql-spark_2.x-x.x.x-SNAPSHOT.jar
```

按如下指令运行(假设你在项目根目录)：

```shell
mkdir -p mlsql-1.1.7/libs

cp streamingpro-mlsql/target/streamingpro-mlsql-spark_2.x-x.x.x-SNAPSHOT.jar mlsql-1.1.7/libs
cp dev/start-local.sh  mlsql-1.1.7

cd  mlsql-1.1.7

# 请确保spark版本和你编译的时候指定的MLSQL_SPARK_VERSIOIN 是一致的
export SPARK_HOME= 你的spark目录

./start-local.sh

```

启动完成后，可以访问http://127.0.0.1:9003，会打开一个页面，在里面可以运行SQL.

## MLSQL-Cluster编译方式

```shell
# clone project
git clone https://github.com/allwefantasy/streamingpro .
cd streamingpro

## build  
mvn -Pcluster-shade -am -pl streamingpro-cluster clean package 
```

编译完成后会有文件： streamingpro-cluster/target/streamingpro-cluster-x.x.x-SNAPSHOT.jar

通过如下方式启动：

```
java -cp .:target/streamingpro-cluster-x.x.x-SNAPSHOT.jar tech.mlsql.cluster.ProxyApplication -config application.yml
```

MLSQL-Cluster 目前只支持接口交互，没有Web页面。
 