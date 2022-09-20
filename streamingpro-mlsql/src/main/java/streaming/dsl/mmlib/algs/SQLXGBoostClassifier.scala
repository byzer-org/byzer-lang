package streaming.dsl.mmlib.algs

import ml.dmlc.xgboost4j.scala.spark.{TrackerConf, XGBoostClassificationModel, XGBoostClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.param.{DoubleParam, FloatParam, IntParam, LongParam, Param, Params}
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.classfication.BaseClassification
import streaming.dsl.mmlib.algs.param.BaseParams

import scala.collection.mutable.ArrayBuffer

/**
 * @author tangfei(tangfeizz@outlook.com)
 * @date 2022/9/5 14:37
 */
class SQLXGBoostClassifier(override val uid: String) extends SQLAlg with MllibFunctions with Functions with BaseClassification {

  // 是否开启网格搜索，默认不开启
  private var cvEnabled = false

  def this() = this(BaseParams.randomUID())

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {

    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean

    cvEnabled = params.getOrElse("cvEnabled", "false").toBoolean

    setKeepVersion(keepVersion)

    val evaluateTable = params.get("evaluateTable")
    setEvaluateTable(evaluateTable.getOrElse("None"))

    SQLPythonFunc.incrementVersion(path, keepVersion)
    val spark = df.sparkSession

    trainModelsWithMultiParamGroupOrCrossValidator(df, path, params, () => {
      getXGBoostClassifier(params)
    }, (_model, fitParam) => {
      evaluateTable match {
        case Some(etable) =>
          val model = _model.asInstanceOf[XGBoostClassificationModel]
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
    val model = load(df.sparkSession, path, params).asInstanceOf[ArrayBuffer[XGBoostClassificationModel]].head
    model.transform(df)
  }

  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession, () => {
      new XGBoostClassifier(Map("tracker_conf" -> TrackerConf(0L, "scala")))
    })
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {

    val (bestModelPath, baseModelPath, metaPath) = mllibModelAndMetaPath(path, params, sparkSession)
    val model = XGBoostClassificationModel.load(bestModelPath(0))
    ArrayBuffer(model)
  }


  override def explainModel(sparkSession: SparkSession, path: String, params: Map[String, String]): DataFrame = {
    val models = load(sparkSession, path, params).asInstanceOf[ArrayBuffer[XGBoostClassificationModel]]
    val rows = models.flatMap { model =>
      val modelParams = model.params.filter(param => model.isSet(param)).map { param =>
        val tmp = model.get(param).get
        val str = if (tmp == null) {
          "null"
        } else tmp.toString
        Seq(("fitParam.[group]." + param.name), str)
      }
      Seq(
        Seq("uid", model.uid)
      ) ++ modelParams
    }.map(Row.fromSeq(_))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rows, 1),
      StructType(Seq(StructField("name", StringType), StructField("value", StringType))))
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    predict_classification(sparkSession, _model, name)
  }

  override def modelType: ModelType = AlgType

  override def doc: Doc = Doc(MarkDownDoc,
    """
      |
      |Xgboost是一个优化的分布式梯度增强库，设计为高效、灵活和可移植，它在梯度增强框架下实现机器学习算法，Xgboost提供并行树增强，也被称为以快速和准确的方式解决许多数据科学问题，相同的代码在主要的分布式环境hadoop上运行，可以解决数十亿例以上的问题
      |
    """.stripMargin)


  override def codeExample: Code = Code(SQLCode, CodeExampleText.jsonStr +
    """
      |-- 测试数据集
      |set jsonStr='''
      |{"features":[5.1,3.5,1.4,0.2],"label":0.0},
      |{"features":[4.9,3.0,1.4,0.2],"label":0.0},
      |{"features":[4.9,3.1,1.5,0.1],"label":0.0},
      |{"features":[5.4,3.7,1.5,0.2],"label":0.0},
      |{"features":[5.1,3.8,1.9,0.4],"label":0.0},
      |{"features":[6.4,3.2,4.5,1.5],"label":1.0},
      |{"features":[6.9,3.1,4.9,1.5],"label":1.0},
      |{"features":[5.7,2.9,4.2,1.3],"label":1.0},
      |{"features":[6.2,2.9,4.3,1.3],"label":1.0},
      |{"features":[5.1,2.5,3.0,1.1],"label":1.0},
      |{"features":[5.7,2.8,4.1,1.3],"label":1.0},
      |{"features":[6.3,3.3,6.0,2.5],"label":2.0},
      |{"features":[7.2,3.6,6.1,2.5],"label":2.0},
      |{"features":[6.2,2.8,4.8,1.8],"label":2.0},
      |{"features":[6.1,3.0,4.9,1.8],"label":2.0}
      |''';
      |-- 加载数据集
      |load jsonStr.`jsonStr` as data;
      |select vec_dense(features) as features ,label as label from data
      |as data1;
      |
      |-- 使用XGBoost算法
      |train data1 as XGBoostClassifier.`/tmp/model` where
      |
      |-- 设置为true,可以保留训练的模型版本
      |keepVersion="true"
      |
      |-- 用于评估的数据集
      |and evaluateTable="data1"
      |
      |-- 用于网格搜索参数，默认是不启用
      |and cvEnabled="false"
      |
      |-- 可以设置多个参数组
      |-- cv开头的参数必须开启cvEnabled="true"
      |-- cv_xx 参数是网格搜索的一般参数
      |-- cv_xgb_xx 参数是xgboost的参数
      |-- cv_grid_xx 参数是网格搜索的范围参数
      |and `fitParam.0.featuresCol`="features"
      |and `fitParam.0.labelCol`="label"
      |and `fitParam.0.cv_xgb_objective`="multi:softprob"
      |and `fitParam.0.cv_xgb_numClass`="3"
      |and `fitParam.0.cv_grid_maxDepth`="2,8"
      |;
    """.stripMargin)

  /**
   * 获取XGBoostClassifier
   *
   * @param params
   * @return
   */
  def getXGBoostClassifier(params: Map[String, String]): Params = {

    var modelParams: Params = new XGBoostClassifier(Map(
      "tracker_conf" -> TrackerConf(0L, "scala")
    ))

    if (cvEnabled) {
      val booster = modelParams.asInstanceOf[XGBoostClassifier]
      modelParams = new CrossValidator()
        .setEstimator(booster)
    }

    modelParams
  }

  /**
   * 获取最佳模型
   *
   * @param alg
   * @param trainData
   * @param fitParam
   * @return
   */
  def getBestXGBoostModel(alg: Params, trainData: DataFrame, fitParam: Map[String, String]): XGBoostClassificationModel = {

    val cvModel: CrossValidator = alg.asInstanceOf[CrossValidator]
    val booster = cvModel.getEstimator.asInstanceOf[XGBoostClassifier]

    //配置网格搜索的一般参数
    if (fitParam.isDefinedAt("cv_numFolds")) {
      cvModel.setNumFolds(fitParam.getOrElse("cv_numFolds", "3").toInt)
    }
    if (fitParam.isDefinedAt("cv_parallelism")) {
      cvModel.setParallelism(fitParam.getOrElse("cv_parallelism", "1").toInt)
    }

    //网格搜索
    val paramGrid = new ParamGridBuilder()
    //配置cvgrid参数
    fitParam.filter(a => a._1.startsWith("cv_grid_")).foreach(f = a => {
      val cvName = a._1.stripPrefix("cv_grid_")
      if (booster.hasParam(cvName)) {
        val para = booster.getParam(cvName)
        val values = a._2.split(",")
        para match {
          case _ if para.isInstanceOf[IntParam] => paramGrid.addGrid(para, values.map(b => b.toInt))
          case _ if para.isInstanceOf[FloatParam] => paramGrid.addGrid(para, values.map(b => b.toFloat))
          case _ => paramGrid.addGrid(para, values)
        }
      }
    })

    //配置xgb参数
    fitParam.filter(a => a._1.startsWith("cv_xgb_")).foreach(a => {
      val cvName = a._1.stripPrefix("cv_xgb_")
      if (booster.hasParam(cvName)) {
        val para = booster.getParam(cvName)
        para match {
          case _ if para.isInstanceOf[IntParam] => booster.set(booster.getParam(cvName), a._2.toInt)
          case _ if para.isInstanceOf[LongParam] => booster.set(booster.getParam(cvName), a._2.toLong)
          case _ if para.isInstanceOf[FloatParam] => booster.set(booster.getParam(cvName), a._2.toFloat)
          case _ if para.isInstanceOf[DoubleParam] => booster.set(booster.getParam(cvName), a._2.toDouble)
          case _ => booster.set(booster.getParam(cvName), a._2)
        }

      }
    })

    //配置模型评估
    val evaluator = new MulticlassClassificationEvaluator()
    if (fitParam.isDefinedAt("cv_evaluator")) {
      evaluator.setMetricName(fitParam.getOrElse("cv_evaluator", "f1"))
    }
    evaluator.setLabelCol("label")
    evaluator.setPredictionCol("prediction")

    cvModel
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid.build())

    val cvm = cvModel.fit(trainData)

    val model = cvm.asInstanceOf[CrossValidatorModel].bestModel.asInstanceOf[XGBoostClassificationModel]

    model
  }

  /**
   * 训练模型，支持多组参数和网格搜索
   *
   * @param df
   * @param path
   * @param params
   * @param modelType
   * @param evaluate
   */
  def trainModelsWithMultiParamGroupOrCrossValidator(df: DataFrame, path: String, params: Map[String, String],
                                                     modelType: () => Params,
                                                     evaluate: (Params, Map[String, String]) => List[MetricValue]
                                                    ) = {

    val keepVersion = params.getOrElse("keepVersion", "true").toBoolean

    val mf = (trainData: DataFrame, fitParam: Map[String, String], modelIndex: Int) => {
      val alg = modelType()
      configureModel(alg, fitParam)

      logInfo(format(s"[training] [alg=${alg.getClass.getName}] [keepVersion=${keepVersion}]"))

      var status = "success"
      var info = ""
      val modelTrainStartTime = System.currentTimeMillis()
      val modelPath = SQLPythonFunc.getAlgModelPath(path, keepVersion) + "/" + modelIndex
      var scores: List[MetricValue] = List()

      var model: XGBoostClassificationModel = null
      try {
        if (cvEnabled) {
          model = getBestXGBoostModel(alg, trainData, fitParam)
        } else {
          model = alg.asInstanceOf[XGBoostClassifier].fit(trainData)
        }
        model.asInstanceOf[MLWritable].write.overwrite().save(modelPath)
        scores = evaluate(model, fitParam)
        logInfo(format(s"[trained] [alg=${alg.getClass.getName}] [metrics=${scores}] [model hyperparameters=${
          model.asInstanceOf[Params].explainParams().replaceAll("\n", "\t")
        }]"))
      } catch {
        case e: Exception =>
          info = format_exception(e)
          logInfo(info)
          status = "fail"
      }
      val modelTrainEndTime = System.currentTimeMillis()

      val metrics = scores.map(score => Row.fromSeq(Seq(score.name, score.value))).toArray
      Row.fromSeq(Seq(
        modelPath.substring(path.length),
        modelIndex,
        alg.getClass.getName,
        metrics,
        status,
        info,
        modelTrainStartTime,
        modelTrainEndTime,
        fitParam))
    }
    var fitParam = arrayParamsWithIndex("fitParam", params)

    if (fitParam.size == 0) {
      fitParam = Array((0, Map[String, String]()))
    }

    val wowRes = fitParam.map { fp =>
      mf(df, fp._2, fp._1)
    }

    val wowRDD = df.sparkSession.sparkContext.parallelize(wowRes, 1)

    df.sparkSession.createDataFrame(wowRDD, StructType(Seq(
      StructField("modelPath", StringType),
      StructField("algIndex", IntegerType),
      StructField("alg", StringType),
      StructField("metrics", ArrayType(StructType(Seq(
        StructField(name = "name", dataType = StringType),
        StructField(name = "value", dataType = DoubleType)
      )))),

      StructField("status", StringType),
      StructField("message", StringType),
      StructField("startTime", LongType),
      StructField("endTime", LongType),
      StructField("trainParams", MapType(StringType, StringType))
    ))).
      write.
      mode(SaveMode.Overwrite).
      parquet(SQLPythonFunc.getAlgMetalPath(path, keepVersion) + "/0")
  }
}
