package streaming.core.ss

import org.apache.spark.sql.ForeachWriter

/**
  * Created by allwefantasy on 17/5/2018.
  */
class NoopForeachWriter[T] extends ForeachWriter[T] {
  override def open(partitionId: Long, version: Long): Boolean = {
    true
  }

  override def process(value: T): Unit = {}

  override def close(errorOrNull: Throwable): Unit = {
  }
}
