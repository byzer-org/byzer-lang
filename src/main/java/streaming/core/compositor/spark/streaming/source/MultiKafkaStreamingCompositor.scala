package streaming.core.compositor.spark.streaming.source

import java.util

import kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.streaming.kafka.KafkaUtils
import serviceframework.dispatcher.{Compositor, Processor, Strategy}
import streaming.common.SQLContextHolder
import streaming.core.compositor.spark.streaming.CompositorHelper
import streaming.core.strategy.platform.SparkStreamingRuntime

import scala.collection.JavaConversions._


class MultiKafkaStreamingCompositor[T] extends Compositor[T] with CompositorHelper {

  private var _configParams: util.List[util.Map[Any, Any]] = _

  val logger = Logger.getLogger(classOf[MultiKafkaStreamingCompositor[T]].getName)

  override def initialize(typeFilters: util.List[String], configParams: util.List[util.Map[Any, Any]]): Unit = {
    this._configParams = configParams
  }

  private def getKafkaParams(params: util.Map[Any, Any]) = {
    params.filter {
      f =>
        if (f._1 == "topics") false else true
    }.toMap.asInstanceOf[Map[String, String]]
  }

  private def zkEnable(params: util.Map[Any, Any]) = {
    getKafkaParams(params).containsKey("zk") ||
      getKafkaParams(params).containsKey("zookeeper") ||
      getKafkaParams(params).containsKey("zkQuorum")
  }

  private def isStreaming(params: util.Map[Any, Any]) = {
    if (params.containsKey("topics")) true else false
  }

  private def getZk(params: util.Map[Any, Any]) = {
    getKafkaParams(params).getOrElse("zk", getKafkaParams(params).getOrElse("zookeeper", getKafkaParams(params).getOrElse("zkQuorum", "127.0.0.1")))
  }

  private def getTopics(params: util.Map[Any, Any]) = {
    params.get("topics").asInstanceOf[String].split(",").toSet
  }

  override def result(alg: util.List[Processor[T]], ref: util.List[Strategy[T]], middleResult: util.List[T], params: util.Map[Any, Any]): util.List[T] = {
    val runtime = params.get("_runtime_").asInstanceOf[SparkStreamingRuntime]
    val ssc = runtime.streamingContext
    val streamings = _configParams.filter(p => isStreaming(p))
    if (streamings.size > 0) {
      new RuntimeException("StreamingPro for now only support multi topics in Kafka instance")
    }
    val p = streamings.head

    val kafkaStream = if (zkEnable(p)) {
      val zk = getZk(p)
      val groupId = getKafkaParams(p).get("groupId").get
      val topics = getTopics(p).map(f => (f, 1)).toMap
      KafkaUtils.createStream(ssc, zk, groupId, topics)
    } else {
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc,
        getKafkaParams(p),
        getTopics(p))
    }

    params.put("mainStream", (rdd: RDD[(String, String)]) => {
      val sqlContext = sqlContextHolder(params)
      val rddRow = rdd.map(f => Row.fromSeq(Seq(f._1, f._2)))
      val df = sqlContext.createDataFrame(rddRow, StructType(Array(StructField("id", StringType, false), StructField("content", StringType))))
      df.registerTempTable(p("outputTable").toString)
    })

    _configParams.filterNot(p => isStreaming(p)).foreach { sourceConfig =>
      val sqlContext = sqlContextHolder(params)
      val sourcePath = if (params.containsKey("streaming.sql.source.path")) params("streaming.sql.source.path").toString else sourceConfig("path").toString
      val df = sqlContext.read.format(sourceConfig("format").toString).options(
        (sourceConfig - "format" - "path" - "outputTable").map(f => (f._1.toString, f._2.toString)).toMap).load(sourcePath)
      df.registerTempTable(sourceConfig("outputTable").toString)
    }

    List(kafkaStream.asInstanceOf[T])
  }

}
