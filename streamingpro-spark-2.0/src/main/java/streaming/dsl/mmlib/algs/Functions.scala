package streaming.dsl.mmlib.algs

import net.csdn.common.logging.Loggers
import org.apache.spark.Partitioner
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.{MLReadable, MLWritable}
import org.apache.spark.sql.{DataFrame, Dataset, functions => F}
import org.apache.spark.util.WowXORShiftRandom
import streaming.common.HDFSOperator

import scala.collection.mutable.ArrayBuffer

/**
  * Created by allwefantasy on 13/1/2018.
  */
trait Functions {
  val logger = Loggers.getLogger(getClass)

  def sampleUnbalanceWithMultiModel(df: DataFrame, path: String, params: Map[String, String], train: (DataFrame, Int) => Unit) = {
    //select count(*) as subLabelCount,label from _ group by labelCol order by  subLabelCount asc
    val labelCol = params.getOrElse("labelCol", "label")
    val labelToCountSeq = df.groupBy(labelCol).agg(F.count(labelCol).as("subLabelCount")).orderBy(F.asc("subLabelCount")).
      select(labelCol, "subLabelCount").collect().map { f =>
      (f.getDouble(0), f.getLong(1))
    }
    val forLog = labelToCountSeq.map(f => s"${f._1}:${f._2}").mkString(",")
    logger.info(s"computing data stat:${forLog}")
    val labelCount = labelToCountSeq.size

    val dfWithLabelPartition = df.rdd.map { f =>
      (f.getAs[Double](labelCol).toInt, f)
    }.partitionBy(new Partitioner {
      override def numPartitions: Int = labelCount

      override def getPartition(key: Any): Int = {
        key.asInstanceOf[Int]
      }
    }).cache()

    try {
      val minLabel = labelToCountSeq.head._1
      val minCount = labelToCountSeq.head._2

      val maxLabel = labelToCountSeq.last._1
      val maxCount = labelToCountSeq.last._2

      val times = (maxCount.toDouble / minCount).ceil

      val labelToCountMapBr = df.sparkSession.sparkContext.broadcast(labelToCountSeq.map { f =>
        //sample rate
        (f._1, minCount.toDouble / f._2)
      }.toMap)
      val forLog2 = labelToCountMapBr.value.map(f => s"${f._1}:${f._2}").mkString(",")
      logger.info(s"all label sample rate:${forLog2}")

      (0 until times.toInt).foreach { time =>
        val tempRdd = dfWithLabelPartition.mapPartitionsWithIndex { (label, iter) =>
          val wow = new WowXORShiftRandom()
          iter.filter(k => wow.nextDouble <= labelToCountMapBr.value(label)).map(f => f._2)
        }
        val trainData = df.sparkSession.createDataFrame(tempRdd, df.schema)
        logger.info(s"training model :${time}")
        train(trainData, time)
      }
    } finally {
      dfWithLabelPartition.unpersist(false)
    }
  }

  def configureModel(model: Params, params: Map[String, String]) = {
    model.params.map { f =>
      if (params.contains(f.name)) {
        val v = params(f.name)
        val m = model.getClass.getMethods.filter(m => m.getName == s"set${f.name.capitalize}").head
        val pt = m.getParameterTypes.head
        val v2 = pt match {
          case i if i.isAssignableFrom(classOf[Int]) => v.toInt
          case i if i.isAssignableFrom(classOf[Double]) => v.toDouble
          case i if i.isAssignableFrom(classOf[Float]) => v.toFloat
          case i if i.isAssignableFrom(classOf[Boolean]) => v.toBoolean
          case _ => v
        }
        m.invoke(model, v2.asInstanceOf[AnyRef])
      }
    }
  }

  def mapParams(name: String, params: Map[String, String]) = {
    params.filter(f => f._1.startsWith(name + ".")).map(f => (f._1.split("\\.").drop(1).mkString("."), f._2))
  }

  def arrayParams(name: String, params: Map[String, String]) = {
    params.filter(f => f._1.startsWith(name + ".")).map { f =>
      val Array(name, group, key) = f._1.split("\\.")
      (group, key, f._2)
    }.groupBy(f => f._1).map { f => f._2.map(k => (k._2, k._1)).toMap }.toArray
  }

  def getModelConstructField(model: Any, modelName: String, fieldName: String) = {
    val modelField = model.getClass.getDeclaredField("org$apache$spark$ml$feature$" + modelName + "$$" + fieldName)
    modelField.setAccessible(true)
    modelField.get(model)
  }

  def getModelField(model: Any, fieldName: String) = {
    val modelField = model.getClass.getDeclaredField(fieldName)
    modelField.setAccessible(true)
    modelField.get(model)
  }

  def loadModels(path: String, modelType: (String) => Any) = {
    val files = HDFSOperator.listModelDirectory(path)
    val models = ArrayBuffer[Any]()
    files.foreach { f =>
      val model = modelType(f.getPath.toString)
      models += model
    }
    models
  }

  def trainModels[T <: Model[T]](df: DataFrame, path: String, params: Map[String, String], modelType: () => Params) = {

    def f(trainData: DataFrame, modelIndex: Int) = {
      val alg = modelType()
      configureModel(alg, params)
      val model = alg.asInstanceOf[Estimator[T]].fit(trainData)
      model.asInstanceOf[MLWritable].write.overwrite().save(path + "/" + modelIndex)
    }
    params.getOrElse("multiModels", "false").toBoolean match {
      case true => sampleUnbalanceWithMultiModel(df, path, params, f)
      case false =>
        f(df, 0)
    }
  }
}
