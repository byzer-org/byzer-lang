package streaming.test

import net.sf.json.{JSONArray, JSONObject}
import streaming.bean.DeployParameterService
import streaming.common.ParamsUtil
import streaming.db.{DB, ManagerConfiguration, TSparkJobParameter, TSparkJobParameterValues}

import scala.io.Source

/**
  * Created by allwefantasy on 14/7/2017.
  */
object SparkParameterInitialize {
  ManagerConfiguration.config = new ParamsUtil(Array(
    "-jdbcPath", "/tmp/jdbc.properties",
    "-yarnUrl", "",
    "-afterLogCheckTimeout", "5",
    "-enableScheduler", "false"
  ))

  DB

  def main(args: Array[String]): Unit = {
//    DeployParameterService.installSteps("spark").foreach { f =>
//      val item = new TSparkJobParameter(-1,
//        f.name,
//        f.parentName,
//        f.parameterType,
//        f.app,
//        f.desc,
//        f.label,
//        f.priority,
//        f.formType,
//        f.actionType,
//        f.comment,
//        f.value
//      )
//      //TSparkJobParameter.save(item)
//
//    }
//    val str = Source.fromInputStream(this.getClass.getResourceAsStream("/sparkParameter.json")).getLines().mkString("\n")
//    import scala.collection.JavaConversions._
//    JSONObject.fromObject(str).foreach {
//      f =>
//        val item = (f._1.asInstanceOf[String], f._2.asInstanceOf[JSONArray].map(f => f.asInstanceOf[String]).toList)
//        val temp = new TSparkJobParameterValues(-1, item._1, 1, item._2.mkString(","))
//        TSparkJobParameterValues.save(new TSparkJobParameterValues(-1, item._1, 0, item._2.mkString(",")))
//
//    }

  }
}
