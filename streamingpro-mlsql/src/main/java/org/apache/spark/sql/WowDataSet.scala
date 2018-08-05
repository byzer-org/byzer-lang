package org.apache.spark.sql

import java.io.CharArrayWriter

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.json.{JSONOptions, JacksonGenerator}
import org.joda.time.DateTime

/**
  * Created by allwefantasy on 3/8/2018.
  */
object WowDataSet {
  def toJSON(d: DataFrame): Array[String] = {
    println(DateTime.now.toString("yyyy-MM-dd HH:mm:ss,SSS"))
    val rowSchema = d.schema
    val sessionLocalTimeZone = d.sparkSession.sessionState.conf.sessionLocalTimeZone
    val rdd: RDD[String] = d.queryExecution.toRdd.mapPartitions { iter =>
      val writer = new CharArrayWriter()
      // create the Generator without separator inserted between 2 records
      val gen = new JacksonGenerator(rowSchema, writer,
        new JSONOptions(Map.empty[String, String], sessionLocalTimeZone))
      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): String = {
          gen.write(iter.next())
          gen.flush()

          val json = writer.toString
          if (hasNext) {
            writer.reset()
          } else {
            gen.close()
          }
          json
        }
      }
    }
    println(DateTime.now.toString("yyyy-MM-dd HH:mm:ss,SSS"))
    rdd.collect()
  }
}
