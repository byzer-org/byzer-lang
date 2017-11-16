package org.apache.spark.sql.execution.datasources.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.json4s.DefaultFormats

import scala.collection.JavaConversions._

/**
  * Created by allwefantasy on 8/3/2017.
  */
class DefaultSource extends RelationProvider with CreatableRelationProvider with DataSourceRegister {

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation =
    HBaseRelation(parameters, None)(sqlContext)

  override def shortName(): String = "hbase"

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val relation = InsertHBaseRelation(data, parameters)(sqlContext)
    relation.createTable(parameters.getOrElse("numReg", "3").toInt)
    relation.insert(data, false)
    relation
  }
}

case class InsertHBaseRelation(
                                dataFrame: DataFrame,
                                parameters: Map[String, String]
                              )(@transient val sqlContext: SQLContext)
  extends BaseRelation with InsertableRelation with Logging {


  private val wrappedConf = {
    implicit val formats = DefaultFormats

    // task is already broadcast; since hConf is per HBaseRelation (currently), broadcast'ing
    // it again does not help - it actually hurts. When we add support for
    // caching hConf across HBaseRelation, we can revisit broadcast'ing it (with a caching
    // mechanism in place)
    new SerializableConfiguration(HBaseConfiguration.create())
  }

  def hbaseConf = wrappedConf.value


  val rowkey = parameters.getOrElse("rowkey", "rowkey")
  val family = parameters.getOrElse("family", "f")
  val outputTableName = parameters("outputTableName")

  def createTable(numReg: Int) {
    val tName = TableName.valueOf(outputTableName)

    val connection = ConnectionFactory.createConnection(hbaseConf)
    // Initialize hBase table if necessary
    val admin = connection.getAdmin
    if (numReg > 3) {
      if (!admin.isTableAvailable(tName)) {
        val tableDesc = new HTableDescriptor(tName)

        val cf = new HColumnDescriptor(Bytes.toBytes(family))
        tableDesc.addFamily(cf)

        val startKey = Bytes.toBytes("aaaaaaa")
        val endKey = Bytes.toBytes("zzzzzzz")
        val splitKeys = Bytes.split(startKey, endKey, numReg - 3)
        admin.createTable(tableDesc, splitKeys)
        val r = connection.getRegionLocator(TableName.valueOf(outputTableName)).getAllRegionLocations
        while (r == null || r.size() == 0) {
          logDebug(s"region not allocated")
          Thread.sleep(1000)
        }
        logInfo(s"region allocated $r")

      }
    } else {
      if (!admin.isTableAvailable(tName)) {
        val tableDesc = new HTableDescriptor(tName)

        val cf = new HColumnDescriptor(Bytes.toBytes(family))
        tableDesc.addFamily(cf)
        admin.createTable(tableDesc)
      }
    }
    admin.close()
    connection.close()
  }

  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName)
    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[String]])

    val fields = data.schema.toArray
    val rowkeyIndex = fields.zipWithIndex.filter(f => f._1.name == rowkey).head._2
    val otherFields = fields.zipWithIndex.filter(f => f._1.name != rowkey)

    val rdd = data.rdd //df.queryExecution.toRdd
    //val schema = data.schema.zipWithIndex.map(f=>(f))
    val f = family

    def convertToPut(row: Row) = {

      val put = new Put(Bytes.toBytes(row.getString(rowkeyIndex)))
      otherFields.foreach { field =>
        if (row.getString(field._2) != null) {
          put.addColumn(Bytes.toBytes(f), Bytes.toBytes(field._1.name), Bytes.toBytes(row.getString(field._2)))
        }
      }
      (new ImmutableBytesWritable, put)
    }
    rdd.map(convertToPut).saveAsNewAPIHadoopDataset(job.getConfiguration)
  }

  override def schema: StructType = dataFrame.schema
}

case class HBaseRelation(
                          parameters: Map[String, String],
                          userSpecifiedschema: Option[StructType]
                        )(@transient val sqlContext: SQLContext)
  extends BaseRelation with TableScan with Logging {

  val hbaseConf = HBaseConfiguration.create()


  def buildScan(): RDD[Row] = {
    hbaseConf.set(TableInputFormat.INPUT_TABLE, parameters("inputTableName"))
    val hBaseRDD = sqlContext.sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      .map { line =>
        val rowKey = Bytes.toString(line._2.getRow)

        import net.liftweb.{json => SJSon}
        implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)

        val content = line._2.getMap.navigableKeySet().flatMap { f =>
          line._2.getFamilyMap(f).map { c =>
            (Bytes.toString(f) + ":" + Bytes.toString(c._1), Bytes.toString(c._2))
          }
        }.toMap

        val contentStr = SJSon.Serialization.write(content)

        Row.fromSeq(Seq(UTF8String.fromString(rowKey), UTF8String.fromString(contentStr)))
      }
    hBaseRDD
  }


  override def schema: StructType = {
    import org.apache.spark.sql.types._
    StructType(
      Array(
        StructField("rowkey", StringType, false),
        StructField("content", StringType)
      )
    )

  }
}

