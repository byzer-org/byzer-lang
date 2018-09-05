## Compilation

Step 1： download & compile ServiceFramework project

```
git clone https://github.com/allwefantasy/ServiceFramework.git
cd ServiceFramework
mvn install -Pscala-2.11 -Pjetty-9 -Pweb-include-jetty-9
```


Step 2： download & compile streamingpro project

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  \
-pl streamingpro-mlsql -am  \
-Ponline -Pscala-2.11  \
-Phive-thrift-server \
-Pspark-2.2.0 \
-Pdsl-legacy \
-Pcrawler \
-Pshade \
-Popencv-support \
-Pcarbondata \
-Pstreamingpro-spark-2.2.0-adaptor

```
 
-Pcarbondata/-Popencv-support are  optional. There are some unittest may lies on streamingpro-opencv module,
you can just delete them. 

Once you see maven complains about streamingpro-dsl, you can do like following:

```
cd streamingpro-dsl
mvn install
cd ../streamingpro-dsl-legacy
mvn install
```

If there are any other question, this link https://github.com/allwefantasy/streamingpro/issues/120 may helps.  

Step 3： run it

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

Now you can interactive with StreamingPro server based on http protocol.


## Tips

ServiceFramework is build by Scala 2.11 by default. You can switch to scala 2.10 by doing this:

```
git clone https://github.com/allwefantasy/ServiceFramework.git
cd ServiceFramework
./dev/change-version-to-2.10.sh
mvn install -Pscala-2.10 -Pjetty-9 -Pweb-include-jetty-9
```

Remember that StreamingPro is run on JDK 1.8. It's required especially when you have streamingpro-crawler moudle enabled.  
If you really need to downgrade to jdk 1.6/1.7 , try modify the pom file like this:

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


You an compile StreamingPro with  lasted spark 2.3.x version:

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package \
-pl streamingpro-mlsql -am  \
-Ponline \
-Pscala-2.11 \
-Phive-thrift-server \
-Pspark-2.3.0 \
-Pdsl  \
-Pshade \
-Pcarbondata \
-Pcrawler  \
-Popencv-support \
-Pstreamingpro-spark-2.3.0-adaptor 

```

Suppose you want compile StreamingPro with spark 2.1.0 ，please delete `streaming.dsl.mmlib.algs.SQLFPGrowth`
then run commnd like this:

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  \
-pl streamingpro-mlsql \
-am  \
-Ponline \
-Pscala-2.11  \
-Phive-thrift-server \
-Pspark-2.1.0 \
-Pdsl-legacy \
-Pshade \
-Pstreamingpro-spark-2.2.0-adaptor

```


You may try with spark 1.6.1, but notice that we stop the maintainment of this version. 

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  -pl streamingpro-spark -am  -Ponline -Pscala-2.10  -Pcarbondata -Phive-thrift-server -Pspark-1.6.1 -Pshade
```




## Full feature Compilation

step1: 

```
git clone https://github.com/allwefantasy/streamingpro.git
cd streamingpro
mvn -DskipTests clean package  \
-pl streamingpro-mlsql -am  \
-Ponline -Pscala-2.11  \
-Phive-thrift-server \
-Pspark-2.2.0 \
-Pdsl-legacy \
-Pcrawler \
-Pshade \
-Popencv-support \
-Pcarbondata \
-Pstreamingpro-spark-2.2.0-adaptor

```

step2: Go to https://github.com/allwefantasy/streamingpro/releases and download jars:

1. ansj_seg-5.1.6.jar
2. nlp-lang-1.7.8.jar

With `--jars ansj_seg-5.1.6.jar,nlp-lang-1.7.8.jar` configuration when the server start up: 


step3: 

Active some extra functions when the server start up:

```
-streaming.udf.clzznames "streaming.crawler.udf.Functions,streaming.dsl.mmlib.algs.processing.UDFFunctions"
```
