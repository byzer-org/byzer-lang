# MLSQL数据源开发指南

## 前言

MLSQL支持标准的Spark DataSource数据源。典型使用如下：

```
load hive.`public.test` as test;

set data='''
{"key":"yes","value":"no","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
''';

-- load data as table
load jsonStr.`data` as datasource;

select * from datasource as table1;
```

那么我们如何实现自己的数据源呢？下面我们会分两部分，第一部分是已经有第三方实现了的标准Spark数据源的集成，第二个是你自己创造的新的数据源。

## 标准Spark 数据源的在封装

我们以HBase为例，这是一个已经实现了标准Spark数据源的驱动，对应的类为`org.apache.spark.sql.execution.datasources.hbase`。 现在我们要把他封装成MLSQL能够很好兼容的数据源。

我们先看看具体使用方法

```sql
--设置链接信息
connect hbase where `zk`="127.0.0.1:2181"
and `family`="cf" as hbase1;

-- 加载hbase 表
load hbase.`hbase1:mlsql_example`
as mlsql_example;

select * from mlsql_example as show_data;


select '2' as rowkey, 'insert test data' as name as insert_table;

-- 保存数据到hbase表
save insert_table as hbase.`hbase1:mlsql_example`;
```

为了实现上述MLSQL中的hbase数据源，我们只要实现创建一个类实现一些接口就可以实现上述功能：

```scala
package streaming.core.datasource.impl
class MLSQLHbase(override val uid: String) extends MLSQLSource with MLSQLSink  with MLSQLRegistry with WowParams {
  def this() = this(BaseParams.randomUID())
```

你需要保证你的包名和上面一致，也就是`streaming.core.datasource.impl`或者是`streaming.contri.datasource.impl`,其次类的名字你随便定义，我们这里定义为MLSQLHBase。 他需要实现一些接口：

1. MLSQLSource 定义了数据源的名字，实现类以及如何进行数据装载。
2. MLSQLSink 定义了如何对数据进行存储。
3. MLSQLRegistry 注册该数据源
4. WowParams 可以让你暴露出你需要的配置参数。也就是load/save语法里的where条件。

## 实现load语法

先看看MLSQLSource多有哪些接口要实现：

```sql
 trait MLSQLDataSource {
  def dbSplitter = {
    "."
  }

  def fullFormat: String

  def shortFormat: String

  def aliasFormat: String = {
    shortFormat
  }

}

trait MLSQLSourceInfo extends MLSQLDataSource {
  def sourceInfo(config: DataAuthConfig): SourceInfo

  def explainParams(spark: SparkSession): DataFrame = {
    import spark.implicits._
    spark.createDataset[String](Seq()).toDF("name")
  }
}

trait MLSQLSource extends MLSQLDataSource with MLSQLSourceInfo {
  def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame
}
```
可以看到MLSQLSource 需要实现的方法比较多，我们一个一个来介绍：

```
def dbSplitter = {
    "."
  }

  def fullFormat: String

  def shortFormat: String

  def aliasFormat: String = {
    shortFormat
  }
```
dbSplitter定义了库表的分割符号，默认是`.`,但比如hbase其实是`:`。 fullFormat是你完整的数据源名字，shortFormat则是短名。aliasFormat一般和shortFormat保持一致。

这里我们覆盖实现结果如下：

```scala
 override def fullFormat: String = "org.apache.spark.sql.execution.datasources.hbase"

  override def shortFormat: String = "hbase"

  override def dbSplitter: String = ":"
```
接着是sourceInfo方法，它的作用主要是提取真实的库表，比如hbase的命名空间和表名。这里是我们HBase的实现：

入参config: DataAuthConfig：
 
> config 参数主要有三个值，分别是path, config, 和df . path 其实就是 `load hbase.\`jack\`` ... 中的jack, config 是个Map,
其实就是where条件形成的，df则可以让你拿到spark 对象。

ConnectMeta.presentThenCall 介绍：

> ConnectMeta.presentThenCall 可以让你拿到connect语法里的所有配置选项，然后和你load语法里的where条件进行合并从而拿到所有的配置选项。

```scala
override def sourceInfo(config: DataAuthConfig): SourceInfo = {   
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter, 2)
    } else {
      Array("", config.path)
    }

    var namespace = _dbname

    if (config.config.contains("namespace")) {
      namespace = config.config.get("namespace").get
    } else {
      if (_dbname != "") {
        val format = config.config.getOrElse("implClass", fullFormat)
       //获取connect语法里的信息
        ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
          if (options.contains("namespace")) {
            namespace = options.get("namespace").get
          }
        })
      }
    }

    SourceInfo(shortFormat, namespace, _dbtable)
  }
```

现在实现注册方法：

```scala
override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }
```

大家照着写就行。

最后实现最核心的load方法：

```scala
override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter, 2)
    } else {
      Array("", config.path)
    }

    var namespace = ""

    val format = config.config.getOrElse("implClass", fullFormat)
  // 获取connect语法里的所有配置参数
    if (_dbname != "") {
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        if (options.contains("namespace")) {
          namespace = options("namespace")
        }
        reader.options(options)
      })
    }

    if (config.config.contains("namespace")) {
      namespace = config.config("namespace")
    }

    val inputTableName = if (namespace == "") _dbtable else s"${namespace}:${_dbtable}"

    reader.option("inputTableName", inputTableName)

    //load configs should overwrite connect configs
    reader.options(config.config)
    reader.format(format).load()
  }
```

上面的代码其实就是调用了标准的spark datasource api进行操作的。

## 实现Save语法

```scala
trait MLSQLSink extends MLSQLDataSource {
  def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any
}
```

因为前面我们已经了MLSQLDataSource需要的方法，所以现在我们只要是实现save语法即可，很简单，也是调用标准的datasource api完成写入：


```scala
override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    val Array(_dbname, _dbtable) = if (config.path.contains(dbSplitter)) {
      config.path.split(dbSplitter, 2)
    } else {
      Array("", config.path)
    }

    var namespace = ""

    val format = config.config.getOrElse("implClass", fullFormat)
    if (_dbname != "") {
      ConnectMeta.presentThenCall(DBMappingKey(format, _dbname), options => {
        if (options.contains("namespace")) {
          namespace = options.get("namespace").get
        }
        writer.options(options)
      })
    }

    if (config.config.contains("namespace")) {
      namespace = config.config.get("namespace").get
    }

    val outputTableName = if (namespace == "") _dbtable else s"${namespace}:${_dbtable}"

    writer.mode(config.mode)
    writer.option("outputTableName", outputTableName)
    //load configs should overwrite connect configs
    writer.options(config.config)
    config.config.get("partitionByCol").map { item =>
      writer.partitionBy(item.split(","): _*)
    }
    writer.format(config.config.getOrElse("implClass", fullFormat)).save()
  }
```

## 最后
最后我们定义我们都可以接受那些常用的配置参数

```
override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  final val zk: Param[String] = new Param[String](this, "zk", "zk address")
  final val family: Param[String] = new Param[String](this, "family", "default cf")
```

完整的代码参看： https://github.com/allwefantasy/streamingpro/blob/master/streamingpro-mlsql/src/main/java/streaming/core/datasource/impl/MLSQLHbase.scala

## 实现loadJson

具体的语法如下：

```sql
set data='''
{"key":"yes","value":"no","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
''';

-- load data as table
load jsonStr.`data` as datasource;

select * from datasource as table1;
```

实现相当简单：

```scala
class MLSQLJSonStr(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())


  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val items = cleanBlockStr(context.execListener.env()(cleanStr(config.path))).split("\n")
    val spark = config.df.get.sparkSession
    import spark.implicits._
    reader.options(rewriteConfig(config.config)).json(spark.createDataset[String](items))
  }

  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Unit = {
    throw new RuntimeException(s"save is not supported in ${shortFormat}")
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }

  override def fullFormat: String = "jsonStr"

  override def shortFormat: String = fullFormat

}
```







