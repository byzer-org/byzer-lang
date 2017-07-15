package streaming.core.compositor.spark.api.example

import org.apache.spark.sql.{DataFrame, SQLContext}
import streaming.core.compositor.spark.api.{OutputWriter, Transform}

/**
  * Created by allwefantasy on 15/7/2017.
  */
class TestTransform extends Transform {
  override def process(sQLContext: SQLContext, contextParams: Map[Any, Any], config: Map[String, String]): Unit = {
    sQLContext.sql("select * from test2").createOrReplaceTempView("test3")
  }
}
