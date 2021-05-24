package tech.mlsql.ets

import java.io.InputStream

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.MLSQLSparkUtils
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, functions => f}
import org.kamranzafar.jtar.{TarEntry, TarInputStream}
import tech.mlsql.app.{ResultRender, ResultResp}
import tech.mlsql.tool.{HDFSOperatorV2, SparkTarfileUtil}

import scala.collection.mutable.ArrayBuffer

/**
 * 24/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class ShowFileTable extends ResultRender {
  override def call(d: ResultResp): ResultResp = {
    if (MLSQLSparkUtils.isFileTypeTable(d.df)) {
      val rdd = d.df.repartition(1).sortWithinPartitions(f.col("start").asc).rdd
      val newRdd = rdd.mapPartitions { iter =>
        if (!iter.hasNext) Seq[Seq[String]]().toIterator
        else {
          val inputStream = SparkTarfileUtil.buildInputStreamFromIterator(iter)

          val fileNames = new ArrayBuffer[String]()
          val tarInputStream = new TarInputStream(inputStream)

          var entry: TarEntry = tarInputStream.getNextEntry
          while (entry != null) {
            fileNames += entry.getName
            entry = tarInputStream.getNextEntry
          }
          tarInputStream.close()
          inputStream.close()
          Seq(fileNames.toSeq).toIterator
        }
      }.flatMap(item => item).map(item => Row.fromSeq(Seq(item)))
      val ds = d.df.sparkSession.createDataFrame(newRdd, StructType(Array(StructField("files", StringType))))
      ResultResp(ds, d.name)
    } else d
  }
}
