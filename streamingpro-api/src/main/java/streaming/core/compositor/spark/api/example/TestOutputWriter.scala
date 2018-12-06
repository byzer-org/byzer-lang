package streaming.core.compositor.spark.api.example

import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import streaming.core.compositor.spark.api.OutputWriter

/**
  * Created by allwefantasy on 15/7/2017.
  */
class TestOutputWriter extends OutputWriter {
  override def write(sQLContext: SQLContext, contextParams: Map[Any, Any], config: Map[String, String]): Unit = {
    sQLContext.sparkSession.table(config("inputTableName")).show(100)
  }
}
