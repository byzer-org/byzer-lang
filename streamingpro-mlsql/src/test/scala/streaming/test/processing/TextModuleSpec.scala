package streaming.test.processing

import java.io.File

import net.sf.json.JSONObject
import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, NotToRunTag, SpecFunctions}
import streaming.dsl.ScriptSQLExec

/**
  * Created by allwefantasy on 12/9/2018.
  */
class TextModuleSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig{
  def createFile(path: String, content: String) = {
    val f = new File("/tmp/abc.txt")
    if (!f.exists()) {
      FileUtils.write(f, "天了噜", "utf-8")
    }
  }

  "analysis with dic" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"

      val f = new File("/tmp/abc.txt")
      if (!f.exists()) {
        FileUtils.write(f, "天了噜", "utf-8")
      }

      var sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("token-analysis"), sq)
      val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
      println(res)
      import scala.collection.JavaConversions._
      assume(JSONObject.fromObject(res).getJSONArray("keywords").
        filter(f => f.asInstanceOf[String]
          == "天了噜/userDefine").size > 0)
    }
  }

  "analysis with dic and deduplicate" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"
      var sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("token-analysis-deduplicate"), sq)
      val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
      println(res)
      import scala.collection.JavaConversions._
      assume(JSONObject.fromObject(res).getJSONArray("keywords").
        filter(f => f.asInstanceOf[String]
          == "天了噜/userDefine").size == 1)
    }
  }

  "analysis with dic with n nature include" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"
      var sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("token-analysis-include-n"), sq)
      val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
      println(res)
      import scala.collection.JavaConversions._
      assume(JSONObject.fromObject(res).getJSONArray("keywords").size() == 1)
      assume(JSONObject.fromObject(res).getJSONArray("keywords").
        filter(f => f.asInstanceOf[String]
          == "天才/n").size > 0)
    }
  }

  "extract with dic" should "work fine" taggedAs (NotToRunTag) in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      //需要有一个/tmp/abc.txt 文件，里面包含"天了噜"
      var sq = createSSEL
      sq = createSSEL
      ScriptSQLExec.parse(loadSQLScriptStr("token-extract"), sq)
      val res = spark.sql("select * from tb").toJSON.collect().mkString("\n")
      println(res)
      import scala.collection.JavaConversions._
      assume(JSONObject.fromObject(res).getJSONArray("keywords").
        filter(f => f.asInstanceOf[String].
          startsWith("天了噜")).size > 0)
    }
  }
}
