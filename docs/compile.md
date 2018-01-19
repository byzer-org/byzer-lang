## 编译

步骤一： 下载编译ServiceFramework项目

```
git clone https://github.com/allwefantasy/ServiceFramework.git
cd ServiceFramework
mvn install -Pscala-2.11 -Pjetty-9 -Pweb-include-jetty-9
```
默认是基于Scala 2.11的。如果你想切换到 scala 2.10则使用如下命令：

```
git clone https://github.com/allwefantasy/ServiceFramework.git
cd ServiceFramework
./dev/change-version-to-2.10.sh
mvn install -Pscala-2.10 -Pjetty-9 -Pweb-include-jetty-9
```

步骤2： 下载编译StreamingPro项目

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  -pl streamingpro-spark-2.0 -am  -Ponline -Pscala-2.11  -Phive-thrift-server -Pspark-2.2.0 -Pshade

```

如果你基于spark 2.1.0 ，你需要删除一个类（streaming.dsl.mmlib.algs.SQLFPGrowth）,之后可以执行如下命令：

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  -pl streamingpro-spark-2.0 -am  -Ponline -Pscala-2.11  -Phive-thrift-server -Pspark-2.1.0 -Pshade

```

默认是编译支持的都是Spark 2.x版本的。如果你希望支持Spark 1.6.x 版本，那么可以采用如下指令：

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  -pl streamingpro-spark -am  -Ponline -Pscala-2.10  -Pcarbondata -Phive-thrift-server -Pspark-1.6.1 -Pshade
```
