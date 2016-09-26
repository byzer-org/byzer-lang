package org.apache.spark.sql.execution.datasources.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.kafka.KafkaClusterService
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, DataSourceRegister, RelationProvider, TableScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.unsafe.types.UTF8String

/**
 * 9/26/16 WilliamZhu(allwefantasy@gmail.com)
 */
class DefaultSource extends RelationProvider with DataSourceRegister {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path", sys.error("'path' must be specified for HDFS data."))
    val fieldName = parameters.getOrElse("fieldName", "raw")
    val kafkaParams: Map[String, String] = parameters.filter { f =>
        f._1 != "path" &&
        f._1 != "fieldName" &&
        f._1 != "format"
    }
    new KafkaRelation(path, fieldName, kafkaParams)(sqlContext)
  }

  override def shortName(): String = "kafka"
}

private[sql] class KafkaRelation(val path: String,
                                 val fieldName: String,
                                 val kafkaParams: Map[String, String]
                                  )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan {
  override def schema: StructType = StructType(Seq(StructField(fieldName, StringType, false)))

  override def buildScan(): RDD[Row] = {
    val sc = sqlContext.sparkContext

    val topics = kafkaParams.get("topics").asInstanceOf[String].split(",").toSet
    val newKafkaParams = kafkaParams.filter(f => f._1 != "topics");


    val fromOffsets = KafkaClusterService.restoreKafkaOffsetFromHDFS(sc, path, "")
    val toOffsets = KafkaClusterService.latestKafkaOffsetFromKafkaCluster(
      newKafkaParams,
      topics)



    val rdd = KafkaUtils.createRDD[String, String, StringDecoder, StringDecoder](
      sc,
      newKafkaParams,
      KafkaClusterService.kafkaRange(toOffsets, fromOffsets)).map { line =>
      Row.fromSeq(Seq(UTF8String.fromString(line._2)))
    }

    rdd
  }
}
