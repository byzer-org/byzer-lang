package streaming.rest

import net.csdn.annotation.rest.At
import net.csdn.modules.http.ApplicationController
import net.csdn.modules.http.RestRequest.Method._
import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.ml.linalg.Vectors
import streaming.core.strategy.platform.{PlatformManager, SparkRuntime}

import scala.collection.JavaConversions._


/**
  * Created by allwefantasy on 20/4/2018.
  */
class RestPredictController extends ApplicationController {

  @At(path = Array("/model/predict"), types = Array(GET, POST))
  def modelPredict = {
    val res = param("dataType", "vector") match {
      case "vector" => vec2vecPredict
      case "string" => string2vecPredict
    }
    render(200, res)
  }

  def getSQL = {
    if (hasParam("sql")) {
      param("sql", "").split("select").mkString("")
    } else if (hasParam("pipeline")) {
      param("pipeline", "").split(",").reverse.foldRight("feature") { (acc, o) =>
        s"${acc}(${o})"
      } + " as feature"
    } else throw new IllegalArgumentException("parameter sql or pipline is required")
  }

  def vec2vecPredict = {
    //dense or sparse
    val vectorType = param("vecType", "dense")
    val sparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
    val vectors = JSONArray.fromObject(param("data", "[]")).map { f =>

      val vec = vectorType match {
        case "dense" =>
          val v = f.asInstanceOf[JSONArray].map(f => f.asInstanceOf[Number].doubleValue()).toArray
          Vectors.dense(v)
        case "sparse" =>
          val v = f.asInstanceOf[JSONObject].map(f => (f._1.asInstanceOf[Int], f._2.asInstanceOf[Number].doubleValue())).toMap
          require(paramAsInt("vecSize", -1) != -1, "when vector type is sparse, vecSize is required")
          Vectors.sparse(paramAsInt("vecSize", -1), v.keys.toArray, v.values.toArray)
      }
      Feature(feature = vec)
    }
    import sparkSession.implicits._
    //select vec_argmax(tf_predict(feature,"feature","label",2)) as predict_label
    val sql = getSQL
    val res = sparkSession.createDataset(sparkSession.sparkContext.parallelize(vectors)).selectExpr(sql).toJSON.collect().mkString(",")
    res

  }

  def string2vecPredict = {
    val sparkSession = runtime.asInstanceOf[SparkRuntime].sparkSession
    val strList = JSONArray.fromObject(param("data", "[]")).map(f => StringFeature(f.toString))
    val sql = getSQL
    import sparkSession.implicits._
    val res = sparkSession.createDataset(sparkSession.sparkContext.parallelize(strList)).selectExpr(sql).toJSON.collect().mkString(",")
    res

  }

  def runtime = PlatformManager.getRuntime
}

