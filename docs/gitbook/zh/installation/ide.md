#MLSQL IDE开发环境配置

>作者：dongbin_
 链接：https://www.jianshu.com/p/4259fb891790
 来源：简书
 简书著作权归作者所有，任何形式的转载都请联系作者获得授权并注明出处。

MLSQL是一个非常棒的项目，为了更加了解该项目，我需要建立MLSQL的开发调试环境，由于项目目前已经比较成熟，搭建环境其实也比较简单，这里记录一下供参考。

MLSQL大部分代码是scala编写的，采用maven管理整个项目，我使用的IDE是idea2018.2.1版本，整个过程跟一般的java项目导入没什么区别。

```
java版本jdk1.8

maven版本3.5.2

```

* * *

#### 第一步：把idea的scala插件加上

左上角File->Settings->Plugins,框里搜索一下scala，如果没有安装搜不到结果，你点击下面中间的Browse repositories,往下翻，找到下图这个安装，安装完需要重启生效。

![image](http://upload-images.jianshu.io/upload_images/3846696-1641b6da4db60791.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/969)

![image](http://upload-images.jianshu.io/upload_images/3846696-de77eec019c2db26.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

#### 第二步：下载代码导入ide

我体验的是这个[TRY分支](https://github.com/allwefantasy/streamingpro/tree/TRY)，下载好代码之后，就可以导入IDE了
File->New->Project from Existing sources,然后选中streamingpro-TRY目录名，导成maven项目

![image](http://upload-images.jianshu.io/upload_images/3846696-ab6558be88ae9b5d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/970)

![image](http://upload-images.jianshu.io/upload_images/3846696-b70e34972bb9cf0e.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/977)

然后一路next到finish即可

导入之后，IDEA会自动下载依赖，过程可能有点长，不过项目本身已经添加了国内的maven镜像地址，在帮助大家加速依赖的下载。

![image](http://upload-images.jianshu.io/upload_images/3846696-6ac48ad807c62d47.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

由于MLSQL支持多个spark版本和许多特性，在实际命令行编译的时候需要指定profiles
具体用到了哪些profiles，大家可以参考MLSQL的文档编译部分，命令行编译的方式是这样的，比如我要支持spark2.4

```
export MLSQL_SPARK_VERSION=2.4
./dev/package.sh

```

执行一下，会看到具体的mvn命令，那么我们可以根据此设置IDE中的profile

```
mvn clean package -DskipTests -Pscala-2.11 -Ponline -Phive-thrift-server -Pcarbondata -Pcrawler -Pdsl -Pxgboost -Pspark-2.4.0 -Pstreamingpro-spark-2.4.0-adaptor -plstreamingpro-mlsql -am -Pshade

```

具体方式是点击IDEA右侧的maven projects工具按钮，我的电脑屏幕的问题，这俩词被挤到扭曲了，如下图所示，只需要在我们要的profile的名字前面打钩就可以了，打上勾之后，IDEA还有一个依赖下载的过程，因为不同的profile依赖不同。

![image](http://upload-images.jianshu.io/upload_images/3846696-d4fcbcac5dfc5947.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

#### 第三步：idea启动该项目

mlsql-engine本地模式的入口是**LocalSparkServiceApp**，等到profile关联的依赖全部下载完成之后，我们打开这个类，没有依赖错误的话，无需其他配置，就可以尝试启动，如果启动过程中有依赖错误，找不到类的情况，可能是IDEA的依赖还没有下载完成，或者索引还没有建完，这时候，可以等待，或者关闭重新打开这个项目重试。

![image](http://upload-images.jianshu.io/upload_images/3846696-b21997f74e2011f3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

启动之后**4040**端口是spark的ui地址，**9003**是engine的web地址，具体可以参考启动日志.

![image](http://upload-images.jianshu.io/upload_images/3846696-b296d273fe336fd8.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

这时候验证下，打开chrome打开9003端口web ui，点击运行，执行这个例子，第一次执行可能需要一点时间

![image](http://upload-images.jianshu.io/upload_images/3846696-da9ec7991e8a8c98.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

![image](http://upload-images.jianshu.io/upload_images/3846696-aa9bf885f43bc385.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

到此，开发调试环境基本上搭好了。


