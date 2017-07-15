package streaming.core.compositor.spark.api

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.DataStreamWriter

/**
  * Created by allwefantasy on 15/7/2017.
  */
trait SSOutputWriter[T] {
  def write(df: DataFrame): DataStreamWriter[T]
}
