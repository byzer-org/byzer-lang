# MLSQL-ET开发指南

在MLSQL中，ET（Estimator/Transformer的简称）是一个非常重要的概念。通过ET,我们可以完成所有SQL（UDF Script）不能完成的工作，同时
也可以提供更加便利的功能集合。
ET也是实现将算法的特征工程从训练复用到预测时的核心，即大部分特征工程训练完成后都可以转化为一个函数，从而供预测时使用。

在MLSQL中，大部分核心功能都是用ET来实现的，比如PythonAlg等。本节，我们会以随机森林为例，来介绍如何用包装Spark内置的
随机森林算法应用于MLSQL中。

## ET 使用语法

假设我们的算法叫RandomForest，对于算法，我们通常需要有使用方式：

1. 训练。 训练后得到的模型会保存起来。

```sql
-- use RandomForest
train data1 as RandomForest.`/tmp/model` where

-- once set true,every time you run this script, MLSQL will generate new directory for you model
keepVersion="true"

-- specify the test dataset which will be used to feed evaluator to generate some metrics e.g. F1, Accurate
and evaluateTable="data1"

-- specify group 0 parameters
and `fitParam.0.labelCol`="features"
and `fitParam.0.featuresCol`="label"
and `fitParam.0.maxDepth`="2"

-- specify group 1 parameters
and `fitParam.1.featuresCol`="features"
and `fitParam.1.labelCol`="label"
and `fitParam.1.maxDepth`="10"
;
```

这里，我们配置了两组参数，系统会自动将两组参数都跑起来，并且会使用 evaluateTable 提供的数据进行效果计算。keepVersion设置为
true后，会保证每次都会有个新目录。

2. 批量预测

```sql
predict data as RandomForest.`/tmp/model`;
```

系统会自动加载/tmp/model的已经训练好的模型，然后对data数据进行预测。

3. 把模型转化为函数，方便在预测服务中使用。

```sql
register RandomForest.`/tmp/model` as rf_predict;

```

然后自动多了一个叫 rf_predict的函数。

## ET 开发

新建一个项目，然后引入如下依赖：

```
<dependency>
     <groupId>tech.mlsql</groupId>
     <artifactId>streamingpro-mlsql-spark_2.4</artifactId>
     <version>1.1.7.1</version>
     <scope>provided</scope>
</dependency>
```

> 打包后的jar包用 --jars带上即可在MLSQL中使用
 
然后新建一个类，叫做SQLRandomForest，所有的ET都需要继承 `streaming.dsl.mmlib.SQLAlg`

```
class SQLRandomForest extends SQLAlg  {
```

然后你需要实现SQLAlg的所有方法。核心方法是四个：

```scala
  def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame

  def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any

  def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction

  def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val sparkSession = df.sparkSession
    import sparkSession.implicits._
    Seq.empty[(String, String)].toDF("param", "description")
  }
```

1. train 对应前面的train语法。 
2. batchPredict 对应前面的predict方法。 
3. load 和 predict 则对应后面的register语法。

### train

我们看到，train其实接受了很多参数。这些参数都是train里的params传递的。我们看到params的签名是 Map[String, String]，
所以在MLSQL中，所有的属性配置都是字符串。我们先对方法做个解释：

```scala
def train(
df: DataFrame, //需要训练的数据 
path: String,  //训练后模型需要保存的路径
params: Map[String, String]): //所有配置参数 
DataFrame //返回结果一般是显示训练的结果，比如时间，是否成功等等。模型会保存在path里。
```

从这个函数签名，对应下之前的train语法：

```sql
train df as RandomForest.`` where a="a" as result;
```

是不是觉得非常对应了？


前面我们看到可以配置多组参数，这些并不需要我们自己实现，MLSQL已经是实现了基础类型。

```scala
class SQLRandomForest(override val uid: String) extends SQLAlg 
with MllibFunctions // spark mllib相关辅助函数。
with Functions      // 辅助函数
with BaseClassification // 参数比如 keepVersion, fitParam等等 
{
def this() = this(BaseParams.randomUID())
}
```

因为我们使用了Spark MLLib里的params,所以需要override uid。下面是train部分所有的代码，我们
在注释里有详细说明：


```scala   
    
/*
获取keepVersion的值
      另外一种用法如下：
      params.get(command.name).map { s =>
            set(command, s)
            s
          }.getOrElse {
            throw new MLSQLException(s"${command.name} is required")
          }
*/
val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
setKeepVersion(keepVersion)    
    
val evaluateTable = params.get("evaluateTable")
setEvaluateTable(evaluateTable.getOrElse("None"))

/*
 递增版本
*/
SQLPythonFunc.incrementVersion(path, keepVersion)
val spark = df.sparkSession

/*
  多组参数解析，包装Mllib里的算法，最后进行训练，并且计算效果
*/
trainModelsWithMultiParamGroup[RandomForestClassificationModel](df, path, params, () => {
  new RandomForestClassifier()
}, (_model, fitParam) => {
  evaluateTable match {
    case Some(etable) =>
      val model = _model.asInstanceOf[RandomForestClassificationModel]
      val evaluateTableDF = spark.table(etable)
      val predictions = model.transform(evaluateTableDF)
      multiclassClassificationEvaluate(predictions, (evaluator) => {
        evaluator.setLabelCol(fitParam.getOrElse("labelCol", "label"))
        evaluator.setPredictionCol("prediction")
      })

    case None => List()
  }
}
)
/*
      输出训练结果
    */
formatOutput(getModelMetaData(spark, path))

```

我们看到，整个过程非常套路，这意味着任何Spark的内置算法都可以用类似的方式呗封装，自己需哟啊开发的代码很少。
在这里，我们也可以看到，我们拿到了df之后，是可以写任何代码的，灵活度非常高。

### 加载模型

一旦模型训练完成之后，我们会把训练参数，模型等都保存起来。之后我们需要加载这些信息，用于预测时使用。
加载的方式如下：

```scala 
override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    /*
     可以加载任意spark内置模型，返回多个模型路径，元数据路径等。我们一般取第一个（已经根据evaluateTable进行了打分，打分
     最高的排在最前面），最后加载成模型     
    */
    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val model = RandomForestClassificationModel.load(bestModelPath(0))
    ArrayBuffer(model)
  }

```

### 批量预测

批量预测本质是调用load得到模型，然后调用spark内置的transform方法。

```scala
 override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val model = load(df.sparkSession, path, params).asInstanceOf[ArrayBuffer[RandomForestClassificationModel]].head
    model.transform(df)
  }
```

### 模型转函数

```scala
override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }

```

我们看看 predict_classification内部：

```scala
def predict_classification(sparkSession: SparkSession, _model: Any, name: String) = {
 
    //把模型广播出去
    val models = sparkSession.sparkContext.broadcast(_model.asInstanceOf[ArrayBuffer[Any]])

    val raw2probabilityMethod = if (sparkSession.version.startsWith("2.3")) "raw2probabilityInPlace" else "raw2probability"
    //这里使用了反射去获取模型里的预测函数，然后封装成spark里的udf
    val f = (vec: Vector) => {
      models.value.map { model =>
        val predictRaw = model.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model, vec).asInstanceOf[Vector]
        val raw2probability = model.getClass.getMethod(raw2probabilityMethod, classOf[Vector]).invoke(model, predictRaw).asInstanceOf[Vector]
        //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
        //概率，分类
        (raw2probability(raw2probability.argmax), raw2probability)
      }.sortBy(f => f._1).reverse.head._2
    }

    val f2 = (vec: Vector) => {
      models.value.map { model =>
        val predictRaw = model.getClass.getMethod("predictRaw", classOf[Vector]).invoke(model, vec).asInstanceOf[Vector]
        val raw2probability = model.getClass.getMethod(raw2probabilityMethod, classOf[Vector]).invoke(model, predictRaw).asInstanceOf[Vector]
        //model.getClass.getMethod("probability2prediction", classOf[Vector]).invoke(model, raw2probability).asInstanceOf[Vector]
        raw2probability
      }
    }

    sparkSession.udf.register(name + "_raw", f2)

    UserDefinedFunction(f, VectorType, Some(Seq(VectorType)))
  }
```

到现在核心的功能已经开发完，同时，我们强烈建议覆盖一下函数：

```scala
 
  /*
    把所有可用参数进行罗列
   */
  def explainParams(sparkSession: SparkSession): DataFrame = {
    import sparkSession.implicits._
    Seq.empty[(String, String)].toDF("param", "description")
  }

  /*
      把所有模型参数罗列
  */
  def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = {
    import sparkSession.implicits._
    Seq.empty[(String, String)].toDF("key", "value")
  }

  /*
      是否自动补充主目录，大部分都需要。所以保持默认即可。
    */
  def skipPathPrefix: Boolean = false
  
  /*
        是数据预处理还是算法。默认未定义。
      */

  def modelType: ModelType = UndefinedType
  
  /*
          文档
        */
  def doc: Doc = Doc(TextDoc, "")
  
  /*
          代码示例
        */
  def codeExample: Code = Code(SQLCode, "")
  
  /*
          Spark 版本兼容
        */
  def coreCompatibility: Seq[CoreVersion] = {
    Seq(Core_2_2_x, Core_2_3_1, Core_2_3_2, Core_2_3_x, Core_2_4_x)
  }
```

下面是RandomForest的完整代码。


```scala

package streaming.dsl.mmlib.algs

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import streaming.dsl.mmlib._

import scala.collection.mutable.ArrayBuffer
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams

/**
  * Created by allwefantasy on 13/1/2018.
  */
class SQLRandomForest(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseClassification {

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {


    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean
    setKeepVersion(keepVersion)

    val evaluateTable = params.get("evaluateTable")
    setEvaluateTable(evaluateTable.getOrElse("None"))

    SQLPythonFunc.incrementVersion(path, keepVersion)
    val spark = df.sparkSession

    trainModelsWithMultiParamGroup[RandomForestClassificationModel](df, path, params, () => {
      new RandomForestClassifier()
    }, (_model, fitParam) => {
      evaluateTable match {
        case Some(etable) =>
          val model = _model.asInstanceOf[RandomForestClassificationModel]
          val evaluateTableDF = spark.table(etable)
          val predictions = model.transform(evaluateTableDF)
          multiclassClassificationEvaluate(predictions, (evaluator) => {
            evaluator.setLabelCol(fitParam.getOrElse("labelCol", "label"))
            evaluator.setPredictionCol("prediction")
          })

        case None => List()
      }
    }
    )

    formatOutput(getModelMetaData(spark, path))
  }


  override def batchPredict(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val model = load(df.sparkSession, path, params).asInstanceOf[ArrayBuffer[RandomForestClassificationModel]].head
    model.transform(df)
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession, () => {
      new RandomForestClassifier()
    })
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {

    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val model = RandomForestClassificationModel.load(bestModelPath(0))
    ArrayBuffer(model)
  }


  override def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = {
    val models = load(sparkSession, path, params).asInstanceOf[ArrayBuffer[RandomForestClassificationModel]]
    val rows = models.flatMap { model =>
      val modelParams = model.params.filter(param => model.isSet(param)).map { param =>
        val tmp = model.get(param).get
        val str = if (tmp == null) {
          "null"
        } else tmp.toString
        Seq(("fitParam.[group]." + param.name), str)
      }
      Seq(
        Seq("uid", model.uid),
        Seq("numFeatures", model.numFeatures.toString),
        Seq("numClasses", model.numClasses.toString),
        Seq("numTrees", model.treeWeights.length.toString),
        Seq("treeWeights", model.treeWeights.mkString(","))
      ) ++ modelParams
    }.map(Row.fromSeq(_))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows, 1),
      StructType(Seq(StructField("name", StringType), StructField("value", StringType))))
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }

  override def modelType: ModelType = AlgType

  override def doc: Doc = Doc(HtmlDoc,
    """
      | <a href="http://en.wikipedia.org/wiki/Random_forest">Random Forest</a> learning algorithm for
      | classification.
      | It supports both binary and multiclass labels, as well as both continuous and categorical
      | features.
      |
      | Use "load modelParams.`RandomForest` as output;"
      |
      | to check the available hyper parameters;
      |
      | Use "load modelExample.`RandomForest` as output;"
      | get example.
      |
      | If you wanna check the params of model you have trained, use this command:
      |      
      | load modelExplain.`/tmp/model` where alg="RandomForest" as outout;      
      |
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |load jsonStr.`jsonStr` as data;
      |select vec_dense(features) as features ,label as label from data
      |as data1;
      |
      |-- use RandomForest
      |train data1 as RandomForest.`/tmp/model` where
      |
      |-- once set true,every time you run this script, MLSQL will generate new directory for you model
      |keepVersion="true"
      |
      |-- specify the test dataset which will be used to feed evaluator to generate some metrics e.g. F1, Accurate
      |and evaluateTable="data1"
      |
      |-- specify group 0 parameters
      |and `fitParam.0.labelCol`="features"
      |and `fitParam.0.featuresCol`="label"
      |and `fitParam.0.maxDepth`="2"
      |
      |-- specify group 1 parameters
      |and `fitParam.1.featuresCol`="features"
      |and `fitParam.1.labelCol`="label"
      |and `fitParam.1.maxDepth`="10"
      |;
    """.stripMargin)

}

```

如果你的包名为streaming.dsl.mmlib.algs并且以SQL开始，Ext结尾，那么该类会被自动注册。比如假设类名为


```
SQLABCExt
```

那么就可以使用在MLSQL中使用ABCExt这个名字了。

### 另外一个示例

如果包装spark的cache功能呢？下面是完整代码：

```scala
class SQLCacheExt(override val uid: String) extends SQLAlg with WowParams {

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val exe = params.get(execute.name).getOrElse {
      "cache"
    }

    val _isEager = params.get(isEager.name).map(f => f.toBoolean).getOrElse(false)

    if (!execute.isValid(exe)) {
      throw new MLSQLException(s"${execute.name} should be cache or uncache")
    }

    if (exe == "cache") {
      df.persist()
    } else {
      df.unpersist()
    }

    if (_isEager) {
      df.count()
    }
    df
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    throw new RuntimeException("register is not support")
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }

  final val execute: Param[String] = new Param[String](this, "execute", "cache|uncache", isValid = (m: String) => {
    m == "cache" || m == "uncache"
  })

  final val isEager: BooleanParam = new BooleanParam(this, "isEager", "if set true, execute computing right now, and cache the table")


  override def doc: Doc = Doc(MarkDownDoc,
    """
      |SQLCacheExt is used to cache/uncache table.
      |      
      |run table CacheExt.`` where execute="cache" and isEager="true";      
      |
      |If you execute the upper command, then table will be cached immediately, othersise only the second time
      |to use the table you will fetch the table from cache.
      |
      |To release the table , do like this:
      |      
      |run table CacheExt.`` where execute="uncache";      
    """.stripMargin)

  override def modelType: ModelType = ProcessType

  def this() = this(BaseParams.randomUID())

}
```



