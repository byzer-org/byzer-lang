## StreamingPro Manager



StreamingPro中的 streamingpro-manager 提供了部署，管理Spark任务的Web界面。轻量易用。



编译streamingpro-manager:

```
git clone https://github.com/allwefantasy/streamingpro.git
mvn clean package  -pl streamingpro-manager -am  -Ponline -Pscala-2.11  -Pshade
```

之后你应该在streamingpro-manager/target有个jar包,另外streamingpro-manager 的resource-local 目录有个sql文件，大家可以根据其创建表库。

## 启动以及启动参数

```
java -cp ./streamingpro-manager-0.4.15-SNAPSHOT.jar streaming.App \
-yarnUrl yarn resource url地址 比如master.host:8080 \
-jdbcPath  /tmp/jdbc.properties \
-envPath   /tmp/env.properties
```
jdbcPath指定jdbc连接参数，比如：

```
url=jdbc:mysql://127.0.0.1:3306/spark_jobs?characterEncoding=utf8
userName=wow
password=wow
```

你也可以把这些参数写到启动参数里。但是前面加上jdbc.前缀就可。比如：

```
java -cp ./streamingpro-manager-0.4.15-SNAPSHOT.jar streaming.App \
-yarnUrl yarn resource url地址 比如master.host:8080 \
-jdbc.url a \
-jdbc.userName a \
-jdbc. password b
```
envPath 里面放置的是你为了使用spark-submit 需要配置的一些参数，比如：

```
export SPARK_HOME=/opt/spark-2.1.1;export HADOOP_CONF_DIR=/etc/hadoop/conf;cd $SPARK_HOME;
```

我们简单看下streamingpro-manager的界面。

第一个界面是上传Jar包：

![WX20170716-165826@2x.png](http://upload-images.jianshu.io/upload_images/1063603-451bc340880d5c33.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


第二界面是提交任务：

![WX20170716-165856@2x.png](http://upload-images.jianshu.io/upload_images/1063603-564f03134084fc66.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

勾选依赖的jar包，选择主jar包，然后做一些参数配置，然后点击提交会进入一个进度界面。

第三个界面是管理页面。

![WX20170716-165808@2x.png](http://upload-images.jianshu.io/upload_images/1063603-7694d3cfc367e138.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

任务能够被监控是要求已经在Yarn上申请到了applicationid。所以如果提交失败了，点击监控按钮是无效的。如果你的程序已经提交过一次并且获得过applicationid，那么你点击监控后，程序30s会扫描一次，并且自动拉起那些没有在运行的程序（比如失败了或者自己跑完了）。


## StreamingPro json文件编辑器支持

StreamingPro在内部已经用在比较复杂的项目上了。所以导致配置文件巨复杂，之前同事提到这事，然后我自己把配置代码拉下来，看了下确实如此。
一开始想着能否利用其它格式，比如自定义的，或者换成XML/Yaml等，后面发现JSON其实已经算是不错的了，项目大了，怎么着都复杂。
后面反复思量，大致从编辑器这个方向做下enhance,可能可以简化写配置的人的工作量。所以有了这个项目。

因为是StreamingPro的一个辅助工具，所以也就直接开源出来了。代码还比较粗糙，相信后续会不断完善。[streamingpro-editor2](https://github.com/allwefantasy/streamingpro-editor2) 。

jar包下载：到目录  https://pan.baidu.com/s/1jIjylRw 下找到 streamingpro-editor2.jar 文件。


安装：

打开配置界面，选择plugins,然后点选红框，从disk进行安装：

![WX20170405-115306@2x.png](http://upload-images.jianshu.io/upload_images/1063603-03c89f124406666c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

选择你的jar然后restart idea intellij 即可。

使用示例：

新建一文件，举个例子，叫做batch.streamingpro。 看标志，就可以发现这是一个标准的json格式文件。大家会发现菜单栏多了一个选项：


![WX20170405-120006@2x.png](http://upload-images.jianshu.io/upload_images/1063603-04b71c832a9a89d1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

其实就是一个模板功能。

在batch.streamingpro里写填写batch,然后点选 expandCode（你也可以去重新设置一个快捷键），


![WX20170405-120228@2x.png](http://upload-images.jianshu.io/upload_images/1063603-24fdb4113d4f0d1a.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

然后就会自动扩展成：


![WX20170405-120243@2x.png](http://upload-images.jianshu.io/upload_images/1063603-fb0f46f9f34a532c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

把 your-name 换成你需要的job名字。 然后我们填写下数据源


![WX20170405-120420@2x.png](http://upload-images.jianshu.io/upload_images/1063603-e9467eaeef7069d3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

运行expandCode,然后就会自动扩展为：


![WX20170405-120548@2x.png](http://upload-images.jianshu.io/upload_images/1063603-04a7bffef1ed9bbb.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

把鼠标移动到format后的双引号内，点击菜单 Code-> Completition -> Basic (你可以用你的快捷键)，然后就会提示有哪些数据源可以用：


![WX20170405-120607@2x.png](http://upload-images.jianshu.io/upload_images/1063603-47cbc9ef295fe8c4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

如果你大致知道数据源的名称，那么会自动做提示：


![WX20170405-120822@2x.png](http://upload-images.jianshu.io/upload_images/1063603-0f2e559114f03774.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

JDBC的参数其实很多，你也可以通过Code-> Completition -> Basic 来进行提示：


![WX20170405-120937@2x.png](http://upload-images.jianshu.io/upload_images/1063603-576058ac65bac54c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

接着你可以通过相同的方式添加batch.sql,batch.outputs,batch.script,batch.script.df模块,操作也是大体相同的。

SQL编辑支持：

另外streamingpro-editor2也支持sql的编辑。在SQL处点击右键：


![WX20170405-213846@2x.png](http://upload-images.jianshu.io/upload_images/1063603-853504ed8d3dd0d9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

点击第一个item, "sql editor",然后进入编辑界面：


![WX20170405-213721@2x.png](http://upload-images.jianshu.io/upload_images/1063603-def1e59b4babe187.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

目前支持高亮以及换行，双引号自动escape等功能。