## 编译

步骤一： 下载编译ServiceFramework项目

```
git clone https://github.com/allwefantasy/ServiceFramework.git
cd ServiceFramework
mvn install -Pscala-2.11 -Pjetty-9 -Pweb-include-jetty-9
```


步骤2： 下载编译StreamingPro项目

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  \
-pl streamingpro-spark-2.0 -am  \
-Ponline -Pscala-2.11  \
-Phive-thrift-server \
-Pspark-2.2.0 \
-Pdsl-legacy \
-Pcrawler \
-Pshade 

```

如果要开启Carbondata支持，加上 `-Pcarbondata` 即可。

如果提示streamingpro-dsl 依赖包无法找到，那么分别进入 streamingpro-dsl/streamingpro-dsl-legacy执行如下指令：

```
cd streamingpro-dsl
mvn install
cd ../streamingpro-dsl-legacy
mvn install
```

如果还有其他问题，也可以参考这个：https://github.com/allwefantasy/streamingpro/issues/120 

步骤三： 运行起来

```
./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name sql-interactive \
streamingpro-spark-2.0-1.0.0.jar    \
-streaming.name sql-interactive    \
-streaming.job.file.path file:///tmp/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift false \
-streaming.enableHiveSupport true
```

现在就可以使用http进行交互了。其中query.json为一个只包含"{}"的配置文件。


## 常见问题

ServiceFramework，默认是基于Scala 2.11的。如果你想切换到 scala 2.10则使用如下命令：

```
git clone https://github.com/allwefantasy/ServiceFramework.git
cd ServiceFramework
./dev/change-version-to-2.10.sh
mvn install -Pscala-2.10 -Pjetty-9 -Pweb-include-jetty-9
```


最新版本StreamingPro已经把语言级别改成了jdk 1.8,当你开启streamingpro-crawler时，这是必要的。
如果你不需要爬虫相关支持，那么可以将根目录下的pom.xml文件中的plugin做降级，
譬如改成jdk source 为 1.6:

```
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <version>2.3.2</version>
    <configuration>
        <!--<compilerArgument>-parameters</compilerArgument>-->
        <compilerArgument>-g</compilerArgument>
        <verbose>true</verbose>
        <source>1.6</source>
        <target>1.6</target>
    </configuration>
</plugin>
```


默认编译使用的是spark2.2.0 版本。如果你希望使用最新的spark 2.3.0 版本，则采用如下指令编译。

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  \
-pl streamingpro-spark-2.0 -am  \
-Ponline -Pscala-2.11  \
-Phive-thrift-server \
-Pspark-2.3.0 \
-Pdsl \
-Pcrawler \
-Pshade 

```

如果你想基于spark 2.1.0 ，你需要删除一个类（streaming.dsl.mmlib.algs.SQLFPGrowth）,之后可以执行如下命令：

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  \
-pl streamingpro-spark-2.0 \
-am  \
-Ponline \
-Pscala-2.11  \
-Phive-thrift-server \
-Pspark-2.1.0 \
-Pdsl-legacy \
-Pshade

```

大部分功能都是基于Spark 2.x版本的。如果你希望使用Spark 1.6.x 版本，那么可以采用如下指令：

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  -pl streamingpro-spark -am  -Ponline -Pscala-2.10  -Pcarbondata -Phive-thrift-server -Pspark-1.6.1 -Pshade
```

值得注意的是，StreamingPro已经对spark 1.6.x 版本已经停止维护了。
