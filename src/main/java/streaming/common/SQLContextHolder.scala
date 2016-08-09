package streaming.common

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 * 8/3/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SQLContextHolder(hiveEnable: Boolean, sparkContext: SparkContext) {

  val hiveContextRef = new AtomicReference[HiveContext]()

  def getOrCreate() = {
    synchronized {
      if (hiveEnable) {
        if (hiveContextRef.get() == null) {
          hiveContextRef.set(new org.apache.spark.sql.hive.HiveContext(sparkContext))
        }
        hiveContextRef.get()
      } else {
        SQLContext.getOrCreate(sparkContext)
      }
    }
  }
}

object SQLContextHolder {
  var sqlContextHolder: SQLContextHolder = null

  def setActive(_sqlContextHolder: SQLContextHolder) = {
    sqlContextHolder = _sqlContextHolder
    SQLContext.setActive(_sqlContextHolder.getOrCreate())
  }

  def getOrCreate() = {
    synchronized {
      if (sqlContextHolder == null) throw new RuntimeException("sqlContextHolder is not initialed")
      sqlContextHolder
    }
  }
}

