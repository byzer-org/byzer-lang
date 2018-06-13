package streaming.core

import java.io.File

import net.sf.json.JSONObject
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.dsl.ScriptSQLExec

/**
  * Created by allwefantasy on 26/4/2018.
  */
class UDFSpec extends BasicSparkOperation with SpecFunctions {
  val batchParams = Array(
    "-streaming.master", "local[2]",
    "-streaming.name", "unit-test",
    "-streaming.rest", "false",
    "-streaming.platform", "spark",
    "-streaming.enableHiveSupport", "true",
    "-streaming.spark.service", "false",
    "-streaming.unittest", "true"
  )


  "keepChinese" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val query = "你◣◢︼【】┅┇☽☾✚〓▂▃▄▅▆▇█▉▊▋▌▍▎▏↔↕☽☾の·▸◂▴▾┈┊好◣◢︼【】┅┇☽☾✚〓▂▃▄▅▆▇█▉▊▋▌▍▎▏↔↕☽☾の·▸◂▴▾┈┊啊，..。，！?katty"

      var res = spark.sql(s"""select keepChinese("${query}",false,array()) as jack""").toJSON.collect()
      var keyRes = JSONObject.fromObject(res(0)).getString("jack")
      assume(keyRes == "你好啊")

      res = spark.sql(s"""select keepChinese("${query}",true,array()) as jack""").toJSON.collect()
      keyRes = JSONObject.fromObject(res(0)).getString("jack")
      assume(keyRes == "你好啊，..。，！?")

      res = spark.sql(s"""select keepChinese("${query}",false,array("。")) as jack""").toJSON.collect()
      keyRes = JSONObject.fromObject(res(0)).getString("jack")
      assume(keyRes == "你好啊。")

    }
  }


}
