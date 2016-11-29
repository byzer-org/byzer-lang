package streaming.common

import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
 * 8/3/16 WilliamZhu(allwefantasy@gmail.com)
 */
class SQLContextHolder(hiveEnable: Boolean, sparkContext: SparkContext, hiveOption: Option[Map[String,String]] = None) {

  val hiveContextRef = new AtomicReference[HiveContext]()

  def getOrCreate() = {
    synchronized {
      if (hiveEnable) {
        if (hiveContextRef.get() == null) {
          val hiveContext = hiveOption match {
            case Some(info) =>
              val hiveContext = Class.forName(info("className")).
               getConstructor(classOf[SparkContext],classOf[String],classOf[String]).
              newInstance(sparkContext,info("store"),info("meta")).asInstanceOf[HiveContext]
              if(sparkContext.getConf.contains("spark.deploy.zookeeper.url")){
                hiveContext.setConf("spark.deploy.zookeeper.url",sparkContext.getConf.get("spark.deploy.zookeeper.url"))
              }
              hiveContext
            case None => new org.apache.spark.sql.hive.HiveContext(sparkContext)
          }
          hiveContextRef.set(hiveContext.asInstanceOf[HiveContext])
        }
        hiveContextRef.get()
      } else {
        SQLContext.getOrCreate(sparkContext)
      }
    }
  }

  def clear = {
    hiveContextRef.set(null)
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

