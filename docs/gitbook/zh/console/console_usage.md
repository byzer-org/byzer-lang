# MLSQL Console 介绍

MLSQL 目前已经初步有一套Web Console 供使用。界面相对来说也比较清爽。

## 工作区介绍 

![](http://docs.mlsql.tech/upload_images/1063603-ad4b2e91807adf66.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)


![](http://docs.mlsql.tech/upload_images/1063603-906e3cbed89a5828.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

下面简单介绍下各个工作区：

1. 脚本区，可以管理脚本。
2. 快捷菜单区，这里你可以通过双击或者拖拽到编辑区，从而实现一个快捷方式（自动生成MLSQL代码）
3. 编辑区，主要为了编辑MLSQL的。
4. 命令区， 提供运行，保存脚本的按钮，还可以设置超时时间等。
5. 消息区， 脚本运行时，会有很多信息在这里输出，包括最后的错误。
6. 工具/数据可视化区， 特定SQL语法会触发生成图表。 另外文件上传也是在这个区。
7. 表格区， 脚本运行完成后，一般都是以表格的形式展示。   

下面的章节我们会对几个用户可能比较陌生的工作区做个简要介绍。

## 快捷菜单区使用

单击Quick Menu,

![](https://upload-images.jianshu.io/upload_images/1063603-0d31b1725883e423.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

红色元素都是可以拖拽到脚本编辑区，你也可以直接双击。假设我要加载一个excel文件，将其拖拽到编辑区，

![](http://docs.mlsql.tech/upload_images/1063603-ccff1f8bd6cb37b4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

系统会弹出一个框：

![](http://docs.mlsql.tech/upload_images/1063603-8324618426d5c156.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

选择excel之后会显示更多针对该数据源的可选参数：

![](http://docs.mlsql.tech/upload_images/1063603-c5b87548a5444ad7.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

点击ok之后，系统自动生成MLSQL语句：

![](http://docs.mlsql.tech/upload_images/1063603-246c40cb941abfa0.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

点击Run即可。

如果我想看当前都有哪些任务在运行，拖拽Show jobs，然后点击Run:

![](http://docs.mlsql.tech/upload_images/1063603-acd76c5b8da1511d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

如果我想知道某个ET组件怎么使用，拖拽Show ET Example:

![](http://docs.mlsql.tech/upload_images/1063603-1c2ffa5923260877.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

这里我选择了深度学习组件，点击Ok然后再点击Run,就可以看到这个类的文档和说明了。

![](http://docs.mlsql.tech/upload_images/1063603-52599f8ab926a79d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

文本显示不好，你只要双击即可：

![](http://docs.mlsql.tech/upload_images/1063603-8b7c164529b8388b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

就会以更好的格式展示相关文档和代码。

## 工具/数据可视化区
 
Tools/Dashboard主要有上传工具以及Dashboard两部分组成（当然也包含RawData部分）：

假设我有个目录test2,然后里面有三个文件，一个excel文件：
![](http://docs.mlsql.tech/upload_images/1063603-1842a50a8de64cb1.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

我可以把整个目录上传上去：


![](http://docs.mlsql.tech/upload_images/1063603-9a808c7f48e4f138.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

现在显示成功：

![](http://docs.mlsql.tech/upload_images/1063603-99cbf670ac2f1dd9.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

现在你可以通过!hdfs命令查看自己上传的文件或者目录 

```sql
!hdfs -ls /tmp/upload; 
```

Dashboard主要是显示流式计算情况的，当然也可以绘图。

你可以看到流的进度详情：

![](http://docs.mlsql.tech/upload_images/1063603-cf860cef9ad74fc5.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

当然我这都变成直线了，因为没有数据持续进来。

点击RawData标签，可以看到每个周期详细信息：

![](http://docs.mlsql.tech/upload_images/1063603-a6a993c6850290b4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

那么如何绘制一个图表呢？  在编辑区提供如下代码：

```sql
set abc='''
{ "x": 100, "y": 200, "z": 200 ,"dataType":"A group"}
{ "x": 120, "y": 100, "z": 260 ,"dataType":"B group"}
''';
load jsonStr.`abc` as table1;
select x,y,z,dataType,"scatter" as dash,
map("A group",map("shape","star")) as _dash_config 
from table1 as output;
```

点击运行，然后点击[Tools/Dashboard]/Dashboard,便可以看到图形

![](http://docs.mlsql.tech/upload_images/WX20190819-180034.png)

## 任务实时进度

当你运行一个复杂的任务时，你可以实时看到这个任务的资源消耗以及进度。

![](http://docs.mlsql.tech/upload_images/1063603-086e68544b74df9a.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


## Console 菜单

刚才我们介绍了首页各个区，现在我们看看菜单还有那些选项。

1. Demo Center  
2. Team


### Demo Center 

我们还提供了一个Demo页，方便展示MLSQL具备的能力，也让你可以一步一步照着学习：

![](http://docs.mlsql.tech/upload_images/1063603-d4453efa16e124b0.png)

你可以通过点击Next Step 看下一步的代码，也可以在当前页面运行看看。

### Team

Team 提供权限和人员管理的功能。我们在后面会详细介绍MLSQL Console里的权限体系。

![](http://docs.mlsql.tech/upload_images/WX20190819-180413.png)






