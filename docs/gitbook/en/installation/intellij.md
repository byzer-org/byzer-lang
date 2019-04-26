# Setup MLSQL Engine in Intellij Idea

>Author：dongbin_
 Link：https://www.jianshu.com/p/4259fb891790
 From：jianshu
 简书著作权归作者所有，任何形式的转载都请联系作者获得授权并注明出处。

MLSQL is a great project. In order to know more details of this project, i want to build the develop environment. Cause
this project have be mature, it's easy to achive this. This article is used to record my steps to confiure MLSQL Engine 
with Intellij Idea.

MLSQL is written by Scala， and use maven to manage the project. The version of IDE i'am using is  2018.2.1. 

```
java版本jdk1.8

maven版本3.5.2

```

* * *

#### Step 1：Install Scala plugin

Left corner of the menu: File->Settings->Plugins,search the keyword `scala`，
If there are nothing in search result, click `Browse repositories`,and then you will find it like the following picture. 
After the installation, you should restart your IDE to make it work。

![image](http://upload-images.jianshu.io/upload_images/3846696-1641b6da4db60791.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/969)

![image](http://upload-images.jianshu.io/upload_images/3846696-de77eec019c2db26.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

#### Step 2：Clone the project and then import it.

The edge branch is [TRY](https://github.com/allwefantasy/streamingpro/tree/TRY). Once you have the code downloaded，you can import it like this:
File->New->Project from Existing sources,then choose the directory of streamingpro-TRY:

![image](http://upload-images.jianshu.io/upload_images/3846696-ab6558be88ae9b5d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/970)

![image](http://upload-images.jianshu.io/upload_images/3846696-b70e34972bb9cf0e.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/977)

click  `Next` and `Next` until `Finish`. 

It will take a long time to wait the IDE downloading the dependency.

![image](http://upload-images.jianshu.io/upload_images/3846696-6ac48ad807c62d47.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

MLSQL have compatibility across many spark versions, so you should specify the profile you want. 
You can use the package.sh to find the mvn command.

```
export MLSQL_SPARK_VERSION=2.4
export DRY_RUN=true
./dev/package.sh

```

It should look like this:

```
mvn clean package -DskipTests -Pscala-2.11 -Ponline -Phive-thrift-server  -Pcrawler -Pdsl -Pxgboost -Pspark-2.4.0 -Pstreamingpro-spark-2.4.0-adaptor -plstreamingpro-mlsql -am -Pshade

```

Click the maven projects in the left side of IDE, and check the profiles.


![image](http://upload-images.jianshu.io/upload_images/3846696-d4fcbcac5dfc5947.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

#### Step 3：Start MLSQL Engine locally

The entry class of MLSQL Engine is  **LocalSparkServiceApp**，try right click and click Run in your pop menu。

![image](http://upload-images.jianshu.io/upload_images/3846696-b21997f74e2011f3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)


**4040**is the spark ui's port，**9003** is MLSQL Engine's port:

![image](http://upload-images.jianshu.io/upload_images/3846696-b296d273fe336fd8.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

Visit `http://127.0.0.1:9003` you will see pictures like this:

![image](http://upload-images.jianshu.io/upload_images/3846696-da9ec7991e8a8c98.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

![image](http://upload-images.jianshu.io/upload_images/3846696-aa9bf885f43bc385.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

This means you have successfully setup your IDE env.


