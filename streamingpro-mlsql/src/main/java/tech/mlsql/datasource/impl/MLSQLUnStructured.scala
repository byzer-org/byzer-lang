package tech.mlsql.datasource.impl

import java.io.{BufferedInputStream, ByteArrayOutputStream, FileInputStream, InputStream}

import _root_.streaming.core.datasource._
import _root_.streaming.dsl.ScriptSQLExec
import _root_.streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.types.{BinaryType, LongType, StructField, StructType}
import org.apache.spark.sql.{functions => f, _}
import org.kamranzafar.jtar.{TarEntry, TarInputStream}
import tech.mlsql.common.utils.path.PathFun
import tech.mlsql.tool.{HDFSOperatorV2, TarfileUtil}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class YieldByteArrayOutputStream(size: Int, pop: (Array[Byte], Int, Boolean) => Unit) extends ByteArrayOutputStream(size) {

  override def write(b: Int): Unit = {
    if (count == size) {
      pop(buf, count, false)
      reset()
    }
    buf(count) = b.toByte
    count += 1
  }

  override def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if (count + len >= size) {
      pop(buf, count, false)
      reset()
    }
    System.arraycopy(b, off, buf, count, len)
    count += len
  }

  override def flush(): Unit = {
    pop(buf, count, false)
    reset()
  }

  override def close(): Unit = {
    pop(buf, count, true)
    reset()
  }

}


class MLSQLUnStructured(override val uid: String) extends MLSQLBaseFileSource with WowParams {
  def this() = this(BaseParams.randomUID())

  override def load(reader: DataFrameReader, config: DataSourceConfig): DataFrame = {
    val context = ScriptSQLExec.contextGetOrForTest()
    val targetPath = resourceRealPath(context.execListener, Option(context.owner), config.path)

    val session = config.df.get.sparkSession
    val rdd = session.sparkContext.makeRDD(Seq("1"), 1).mapPartitions { iter =>
      var done = false
      var start = 0L

      val stack = new mutable.Queue[(Long, Long, Array[Byte])]()
      val bytesOutputStream = new YieldByteArrayOutputStream(1024 * 64, (buf: Array[Byte], count: Int, _done) => {
        if (_done) {
          done = _done
        } else {
          while (stack.size > 100) {
            Thread.sleep(10)
          }
          stack.enqueue((start, count, java.util.Arrays.copyOf(buf, count)))
          start = start + count
        }

      })
      TarfileUtil.createTarFileStream(bytesOutputStream, targetPath)

      new Iterator[Row]() {
        override def hasNext: Boolean = !done

        override def next(): Row = {
          val item = stack.dequeue()
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
    config.df.get.repartition(1).sortWithinPartitions(f.col("start").asc).foreachPartition { iter =>
      val fs = FileSystem.get(HDFSOperatorV2.hadoopConfiguration)
      val context = ScriptSQLExec.contextGetOrForTest()
      val targetPath = resourceRealPath(context.execListener, Option(context.owner), config.path)
      var currentBuf = iter.next().getAs[Array[Byte]]("value")
      var currentBufPos = 0
      val inputStream = new InputStream {
        override def read(): Int = {
          if (currentBufPos == currentBuf.length - 1) {
            if (iter.hasNext) {
              currentBuf = iter.next().getAs[Array[Byte]]("value")
            } else {
              return -1
            }
          }
          val b = currentBuf(currentBufPos)
          currentBufPos += 1
          b
        }
      }
      val tarInputStream = new TarInputStream(new BufferedInputStream(inputStream));
      var entry: TarEntry = tarInputStream.getNextEntry
      val fileNames = new ArrayBuffer[String]();
      while (entry != null) {
        entry = tarInputStream.getNextEntry
        fileNames += entry.getName
        val targetFilePath = new Path(PathFun(targetPath).add(entry.getName).toPath)
        if (!fs.exists(targetFilePath.getParent)) {
          fs.mkdirs(targetFilePath.getParent)
        }
        val targetFile = fs.create(targetFilePath, true)
        IOUtils.copy(new FileInputStream(entry.getFile), targetFile)
        targetFile.close()
      }
      tarInputStream.close()
      inputStream.close()
      fileNames.toIterator
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