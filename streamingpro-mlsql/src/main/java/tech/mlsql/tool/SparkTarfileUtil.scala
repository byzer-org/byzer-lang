package tech.mlsql.tool

import com.alibaba.druid.DbType
import org.apache.spark.sql.Row

import java.io.InputStream

/**
 * 2019-05-20 WilliamZhu(allwefantasy@gmail.com)
 */
object SparkTarfileUtil {
  def main(args: Array[String]): Unit = {
    println(DbType.of("mysql") == DbType.mysql)
  }

  def buildInputStreamFromIterator(iter: Iterator[Row]) = {
    var currentBlockRow = iter.next()
    var currentBuf = currentBlockRow.getAs[Array[Byte]]("value")
    var currentBufPos = 0
    val inputStream = new InputStream {
      override def read(): Int = {
        if (currentBufPos == currentBuf.length) {
          val hasNext = iter.hasNext
          if (hasNext) {
            currentBlockRow = iter.next()
            currentBuf = currentBlockRow.getAs[Array[Byte]]("value")
            currentBufPos = 0
          } else {
            return -1
          }
        }
        val b = currentBuf(currentBufPos)
        currentBufPos += 1
        b & 0xFF
      }
    }
    inputStream
  }
}
