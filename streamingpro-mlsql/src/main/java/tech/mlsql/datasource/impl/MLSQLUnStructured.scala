package tech.mlsql.datasource.impl

import java.io.InputStream
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import _root_.streaming.core.datasource._
import _root_.streaming.dsl.ScriptSQLExec
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{BinaryType, LongType, StructField, StructType}
import org.apache.spark.sql.{functions => f, _}
import org.kamranzafar.jtar.{TarEntry, TarInputStream}
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.tool.{HDFSOperatorV2, SparkTarfileUtil, TarfileUtil, YieldByteArrayOutputStream}

import scala.collection.mutable.ArrayBuffer

class MLSQLUnStructured(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val targetPath = resourceRealPath(context.execListener, Option(context.owner), config.path)

    val session = config.df.get.sparkSession
    val rdd = session.sparkContext.makeRDD(Seq("1"), 1).mapPartitions { iter =>
      var done = false
      var start = 0L

      val stack = new LinkedBlockingQueue[(Long, Long, Array[Byte])](100)
      val bytesOutputStream = new YieldByteArrayOutputStream(1024 * 64, (buf: Array[Byte], count: Int, _done) => {
        if (_done) {
          done = _done
        } else if (count > 0) {
          stack.offer((start, count, java.util.Arrays.copyOf(buf, count)), 10, TimeUnit.SECONDS)
          start = start + count
        }

      })
      TarfileUtil.createTarFileStream(bytesOutputStream, targetPath)

      new Iterator[Row]() {
        override def hasNext: Boolean = !done || stack.size() > 0

        override def next(): Row = {
          val item = stack.poll(10, TimeUnit.SECONDS)
          Row.fromSeq(Seq(item._1, item._2, item._3))
        }
      }
    }
    val newDF = session.createDataFrame(rdd, StructType(Array(
      StructField("start", LongType),
      StructField("offset", LongType),
      StructField("value", BinaryType))))
    newDF
  }


  override def save(writer: DataFrameWriter[Row], config: DataSinkConfig): Any = {

    val context = ScriptSQLExec.contextGetOrForTest()
    val targetPath = resourceRealPath(context.execListener, Option(context.owner), config.path)
    val rdd = config.df.get.repartition(1).sortWithinPartitions(f.col("start").asc).rdd
    assert(rdd.partitions.length == 1,"rdd partition num should be 1")
    rdd.foreachPartition { iter =>
      if (!iter.hasNext) Seq[Row]().toIterator
      else {
        val fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration)
        val inputStream = SparkTarfileUtil.buildInputStreamFromIterator(iter)

        val fileNames = new ArrayBuffer[String]()
        val tarInputStream = new TarInputStream(inputStream)
        
        var entry: TarEntry = tarInputStream.getNextEntry
        while (entry != null) {
          fileNames += entry.getName
          val targetFilePath = new Path(PathFun(targetPath).add(entry.getName).toPath)

          if (!fs.exists(targetFilePath.getParent)) {
            fs.mkdirs(targetFilePath.getParent)
          }
          if (!entry.isDirectory) {
            val targetFile = fs.create(targetFilePath, true)
            IOUtils.copy(tarInputStream, targetFile)
            targetFile.close()
          }

          entry = tarInputStream.getNextEntry
        }
        tarInputStream.close()
        inputStream.close()
        fileNames.toIterator
      }

    }
  }

  override def explainParams(spark: SparkSession) = {
    _explainParams(spark)
  }

  override def register(): Unit = {
    DataSourceRegistry.register(MLSQLDataSourceKey(fullFormat, MLSQLSparkDataSourceType), this)
    DataSourceRegistry.register(MLSQLDataSourceKey(shortFormat, MLSQLSparkDataSourceType), this)
  }


  override def fullFormat: String = "unStructured"

  override def shortFormat: String = "unStructured"

}