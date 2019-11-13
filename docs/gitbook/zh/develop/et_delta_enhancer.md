# MLSQL ET插件 deltaenhancer 开发示例

MLSQL提供了 [插件商店](https://docs.mlsql.tech/zh/plugins/),方便开发者发布自己开发的插件。

MLSQL 支持四种类型的插件：

1. ET 插件
2. DataSource 插件
3. Script 插件
4. App 插件


如果从数据处理的角度而言，DataSource插件可以让你扩展MLSQL访问你想要的数据源，而ET插件则可以完成数据处理相关的工作，甚至构建一个机器学习集群，
比如TF Cluster 实际上就是一个ET插件。 Script 插件则是一个更高层次的插件，是可复用的MLSQL代码。

这篇文章，我们将重点介绍如何开发一个ET插件。


这是官网的第一个示例插件：

![](http://docs.mlsql.tech/upload_images/WX20190914-161623@2x.png)

大家可以做在[github上找到项目的源码](https://github.com/allwefantasy/deltaehancer)。

## 新建项目

新建一个Maven项目，引入如下依赖：


```xml
<dependencies>
        <dependency>
            <groupId>tech.mlsql</groupId>
            <artifactId>streamingpro-mlsql-spark_2.4_2.11</artifactId>
            <version>1.5.0-SNAPSHOT</version>
            <scope>provided</scope>
        </dependency>
        <dependency>
            <groupId>tech.mlsql</groupId>
            <artifactId>delta-plus_2.11</artifactId>
            <version>0.1.3</version>
            <scope>provided</scope>
        </dependency>
    </dependencies>
```

因为我们需要开发delta相关的功能，所以我们将 delta-plus也显示的依赖进来了。注意这些依赖都需要使用 `<scope>provided</scope>`,避免打包的时候
打进去。如果还需要其他的一些jar包，请尽可能和MLSQL项目中的保持版本一致，避免版本冲突。


现在可以开始开发代码了，新建一个类`tech.mlsql.plugin.et.DeltaCommand`,他需要继承如下几个类：

```sql
class DeltaCommand(override val uid: String) extends SQLAlg with VersionCompatibility with Functions with WowParams {
  def this() = this(BaseParams.randomUID())
```

其中核心的是SQLAlg 以及 VersionCompatibility。
SQLAlg 指定了哪些业务方法我们必须要实现，VersionCompatibility则让我们指定该插件能够兼容哪些
版本的MLSQL。MLSQL会在加载插件的时候会通过该检查兼容性。


通常而言，我们只要实现两个方法，一个train,一个supportedVersions。

当我们开发完成后，我们立刻可以通过如下方式调用我们的插件

```sql
run command as DeltaCommand.`` where parameters='''{:all}''';
```

当然，通过恰当的设置，我们也可以将其转化为一个命令行，比如:

```sql
!delta_enhancer pruneDeletes db1.table 110000;
```

对应的命令行参数会转化为json数组自动填充到parameters参数里。


## 核心业务逻辑

Train 方法包含了具体的实现逻辑：
```scala
override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val spark = df.sparkSession

    def resolveRealPath(dataPath: String) = {
      val dataLake = new DataLake(spark)
      if (dataLake.isEnable) {
        dataLake.identifyToPath(dataPath)
      } else {
        PathFun(path).add(dataPath).toPath
      }
    }


    val command = JSONTool.parseJson[List[String]](params("parameters"))
    command match {
      case Seq("pruneDeletes", dataPath, howManyHoures, _*) =>
        val deltaLog = DeltaTable.forPath(spark, resolveRealPath(dataPath))
        deltaLog.vacuum(howManyHoures.toInt)
    }

  }
    
```


train方法对应的是train关键字，譬如：

```sql
train command as DeltaCommand.`/tmp/jack` where parameters="[]";
```

train参数中的df指代 command,  path指代 DeltaCommand.`/tmp/jack`中的 /tmp/jack， params则指代where后面的条件。
因为DeltaCommand我最终希望在命令行里使用，所以我设置了一个特殊的参数parameters，如果你使用命令行的时候，MLSQL会自动将命令行参数
转化为json数组放到parameters里去。

所以在train方法中，我们需要核心解析parameters，并且根据具体的解析结果采取相应的逻辑。

为了能够让run关键字也能使用，你还需要实现batchPredict:

```scala
override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = train(df, path, params)
```


现在，你可以这么写了：

```sql
run command as DeltaCommand.`` where parameters="{}";
```


## 版本兼容性

版本兼容性指定则是通过实现supportedVersions方法：

```scala
override def supportedVersions: Seq[String] = {
    Seq("1.5.0-SNAPSHOT", "1.5.0")
  }
```


## 如何部署你的插件

你需要将插件使用shade模式打成jar包然后提交到应用商店，之后用户可以直接使用!plugin命令安装。

```sql
!plugin et add tech.mlsql.plugin.et.DeltaCommand delta_enhancer named deltaEnhancer; 
```


 
