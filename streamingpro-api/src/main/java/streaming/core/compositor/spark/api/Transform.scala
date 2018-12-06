package streaming.core.compositor.spark.api

import org.apache.spark.sql.SQLContext

/**
  * Created by allwefantasy on 15/7/2017.
  */
trait Transform {
  def process(sQLContext: SQLContext, contextParams: Map[Any, Any], config: Map[String, String]): Unit
}
