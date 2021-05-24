package tech.mlsql.ets

import java.io.ByteArrayInputStream

import org.apache.spark.MLSQLSparkUtils
import org.apache.spark.sql.{functions => f}
import tech.mlsql.app.{ResultRender, ResultResp}
import tech.mlsql.tool.TarfileUtil

import scala.collection.JavaConverters._

/**
 * 24/5/2021 WilliamZhu(allwefantasy@gmail.com)
 */
class ShowFileTable extends ResultRender {
  override def call(d: ResultResp): ResultResp = {
    if (MLSQLSparkUtils.isFileTypeTable(d.df)) {
      val newdf = d.df.repartition(1).sortWithinPartitions(f.col("start").asc)
      val bytesArray = newdf.collect().map(_.getAs[Array[Byte]]("value")).reduce((a, b) => a ++ b)
      val fileNames = TarfileUtil.extractTarFile(new ByteArrayInputStream(bytesArray))
      import d.df.sparkSession.implicits._
      val ds = d.df.sparkSession.createDataset[String](fileNames.asScala.toSeq)
      ResultResp(ds.toDF("content"), d.name)
    } else d
  }
}
