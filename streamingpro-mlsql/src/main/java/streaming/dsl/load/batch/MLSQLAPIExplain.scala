package streaming.dsl.load.batch

import net.csdn.ServiceFramwork
import net.csdn.api.controller.APIDescAC
import net.csdn.common.settings.Settings
import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConversions._

/**
  * 2018-11-30 WilliamZhu(allwefantasy@gmail.com)
  */
class MLSQLAPIExplain(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = false

  override def explain: DataFrame = {
    val items = JSONArray.fromObject(APIDescAC.openAPIs(ServiceFramwork.injector.getInstance(classOf[Settings]))).
      flatMap(f => f.asInstanceOf[JSONObject].getJSONArray("actions").map(m => JSONObject.fromObject(m).toString))
    val rows = sparkSession.sparkContext.parallelize(items, 1)
    sparkSession.read.json(rows)
  }
}
