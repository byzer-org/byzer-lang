/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package streaming.dsl.mmlib.algs.param

import net.sf.json.{JSONArray, JSONObject}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.mlsql.session.MLSQLException
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import tech.mlsql.common.form._
import tech.mlsql.common.utils.serder.json.JSONTool
import tech.mlsql.dsl.TagParamName

/**
 * Created by allwefantasy on 20/9/2018.
 */
trait WowParams extends Params {
  override def copy(extra: ParamMap): Params = defaultCopy(extra)


  private lazy final val overwriteParamMapping = Map(
    "featureSubsetStrategy" -> ((param: Param[_], obj: Params) => {
      Array(param.name, param.doc, obj.explainParam(param), FormParams.toJson(
        Select(
          name = param.name, values = List(), extra = Extra(doc = param.doc, label = "", Map(
            "defaultValue" -> obj.getDefault(param).getOrElse("undefined").toString,
            "currentValue" -> obj.get(param).getOrElse("undefined").toString,
            "valueType" -> "string",
            "required" -> "false",
            "derivedType" -> "NONE"
          )), valueProvider = Option(() => {

            Array("auto", "all", "onethird", "sqrt", "log2", "(0.0-1.0]", "[1-n]").map(item =>
              KV(Option(param.name), Option(item))
            ).toList
          })
        )
      ))
    })
  )

  def paramToJSon(param: Param[_], obj: Params): Array[String] = {

    val defaultValueStr = obj.getDefault(param)
    val currentValueStr = obj.get(param)

    val valueStr = if (obj.getDefault(param).isDefined) {
      val defaultValueStr = obj.getDefault(param).map("default: " + _)
      val currentValueStr = obj.get(param).map("current: " + _)
      (defaultValueStr ++ currentValueStr).mkString("(", ", ", ")")
    } else {
      "(undefined)"
    }

    def getExtra(doc: String): (Option[JSONObject], Option[Extra]) = {

      param.doc.trim match {
        case s if s.startsWith("{") && s.endsWith("}") =>
          val json = JSONObject.fromObject(param.doc)
          (Some(json), Some(JSONTool.parseJson[Extra](json.getJSONObject("extra").toString())))

        case s if s.startsWith("[") && s.endsWith("]") =>
          val json = JSONArray.fromObject(param.doc).get(0).asInstanceOf[JSONObject]
          (Some(json), Some(JSONTool.parseJson[Extra](json.getJSONObject("extra").toString())))

        case _ => (None, None)
      }
    }

    val (rawOpt, extraOpt) = getExtra(param.doc)
    val doc = extraOpt.map(_.doc).getOrElse(param.doc)


    val extraJson = extraOpt match {
      case Some(item) =>
        val newItem = item.copy(options = item.options ++ Map(
          "defaultValue" -> defaultValueStr.getOrElse(extraOpt.map(_.options).map(_.getOrElse("defaultValue","undefined")).getOrElse("undefined")).toString,
          "currentValue" -> currentValueStr.getOrElse("undefined").toString,
          "valueType" -> extraOpt.map(_.options).map(_.getOrElse("valueType","string")).getOrElse("string"),
          "required" -> extraOpt.map(_.options).map(_.getOrElse("required","false")).getOrElse("false"),
          "derivedType" -> extraOpt.map(_.options).map(_.getOrElse("derivedType","NONE")).getOrElse("NONE")
        ))
        rawOpt.get.put("extra", JSONObject.fromObject(JSONTool.toJsonStr(newItem)))
        rawOpt.get.toString()
      case None => "{}"
    }
    Array(param.name, doc, valueStr, extraJson)
  }

  def buildInAlgParamToJson(param: Param[_], obj: Params): Array[String] = {

    if (overwriteParamMapping.contains(param.name)) {
      return overwriteParamMapping(param.name)(param, obj)
    }

    param match {
      case a: IntParam =>

        Array(param.name, a.doc, obj.explainParam(param), FormParams.toJson(
          Text(
            name = a.name, value = obj.explainParam(param), extra = Extra(doc = a.doc, label = "", Map(
              "defaultValue" -> obj.getDefault(param).getOrElse("undefined").toString,
              "currentValue" -> obj.get(param).getOrElse("undefined").toString,
              "valueType" -> "int",
              "required" -> "false",
              "derivedType" -> "NONE"
            )), valueProvider = Option(() => {
              obj.getDefault(param).toString
            })
          )
        ))

      case a: FloatParam =>

        Array(param.name, a.doc, obj.explainParam(param), FormParams.toJson(
          Text(
            name = a.name, value = obj.explainParam(param), extra = Extra(doc = a.doc, label = "", Map(
              "defaultValue" -> obj.getDefault(param).getOrElse("undefined").toString,
              "currentValue" -> obj.get(param).getOrElse("undefined").toString,
              "valueType" -> "float",
              "required" -> "false",
              "derivedType" -> "NONE"
            )), valueProvider = Option(() => {
              obj.getDefault(param).toString
            })
          )
        ))

      case a: DoubleParam =>

        Array(param.name, a.doc, obj.explainParam(param), FormParams.toJson(
          Text(
            name = a.name, value = obj.explainParam(param), extra = Extra(doc = a.doc, label = "", Map(
              "defaultValue" -> obj.getDefault(param).getOrElse("undefined").toString,
              "currentValue" -> obj.get(param).getOrElse("undefined").toString,
              "valueType" -> "double",
              "required" -> "false",
              "derivedType" -> "NONE"
            )), valueProvider = Option(() => {
              obj.getDefault(param).toString
            })
          )
        ))

      case a: IntArrayParam =>

        Array(param.name, a.doc, obj.explainParam(param), FormParams.toJson(
          Text(
            name = a.name, value = obj.explainParam(param), extra = Extra(doc = a.doc, label = "", Map(
              "defaultValue" -> obj.getDefault(param).getOrElse("undefined").toString,
              "currentValue" -> obj.get(param).getOrElse("undefined").toString,
              "valueType" -> "array[int]",
              "required" -> "false",
              "derivedType" -> "NONE"
            )), valueProvider = Option(() => {
              obj.getDefault(param).toString
            })
          )
        ))

      case a: DoubleArrayParam =>

        Array(param.name, a.doc, obj.explainParam(param), FormParams.toJson(
          Text(
            name = a.name, value = obj.explainParam(param), extra = Extra(doc = a.doc, label = "", Map(
              "defaultValue" -> obj.getDefault(param).getOrElse("undefined").toString,
              "currentValue" -> obj.get(param).getOrElse("undefined").toString,
              "valueType" -> "array[double]",
              "required" -> "true",
              "required" -> "false",
              "derivedType" -> "NONE"
            )), valueProvider = Option(() => {
              obj.getDefault(param).toString
            })
          )
        ))

      case a: DoubleArrayArrayParam =>

        Array(param.name, a.doc, obj.explainParam(param), FormParams.toJson(
          Text(
            name = a.name, value = obj.explainParam(param), extra = Extra(doc = a.doc, label = "", Map(
              "defaultValue" -> obj.getDefault(param).getOrElse("undefined").toString,
              "currentValue" -> obj.get(param).getOrElse("undefined").toString,
              "valueType" -> "array[array[double]]",
              "required" -> "false",
              "derivedType" -> "NONE"
            )), valueProvider = Option(() => {
              obj.getDefault(param).toString
            })
          )
        ))

      case a: StringArrayParam =>
        Array(param.name, a.doc, obj.explainParam(param), FormParams.toJson(
          Text(
            name = a.name, value = obj.explainParam(param), extra = Extra(doc = a.doc, label = "", Map(
              "defaultValue" -> obj.getDefault(param).getOrElse("undefined").toString,
              "currentValue" -> obj.get(param).getOrElse("undefined").toString,
              "valueType" -> "array[string]",
              "required" -> "false",
              "derivedType" -> "NONE"
            )), valueProvider = Option(() => {
              obj.getDefault(param).toString
            })
          )
        ))
      case a: BooleanParam =>

        Array(param.name, a.doc, obj.explainParam(param), FormParams.toJson(
          Select(
            name = a.name, values = List(), extra = Extra(doc = a.doc, label = "", Map(
              "defaultValue" -> obj.getDefault(param).getOrElse("undefined").toString,
              "currentValue" -> obj.get(param).getOrElse("undefined").toString,
              "valueType" -> "boolean",
              "required" -> "false",
              "derivedType" -> "NONE"
            )), valueProvider = Option(() => {
              List(KV(Option(param.name), Option("true")), KV(Option(param.name), Option("false")))
            })
          )
        ))

      case a: LongParam =>

        Array(param.name, a.doc, obj.explainParam(param), FormParams.toJson(
          Text(
            name = a.name, value = obj.explainParam(param), extra = Extra(doc = a.doc, label = "", Map(
              "defaultValue" -> obj.getDefault(param).getOrElse("undefined").toString,
              "currentValue" -> obj.get(param).getOrElse("undefined").toString,
              "valueType" -> "long",
              "required" -> "false",
              "derivedType" -> "NONE"
            )), valueProvider = Option(() => {
              obj.getDefault(param).toString
            })
          )
        ))

      case a: Param[String] =>

        Array(param.name, a.doc, obj.explainParam(param), FormParams.toJson(
          Text(
            name = a.name, value = obj.explainParam(param), extra = Extra(doc = a.doc, label = "", Map(
              "defaultValue" -> obj.getDefault(param).getOrElse("undefined").toString,
              "currentValue" -> obj.get(param).getOrElse("undefined").toString,
              "valueType" -> "string",
              "required" -> "false",
              "derivedType" -> "NONE"
            )), valueProvider = Option(() => {
              obj.getDefault(param).toString
            })
          )
        ))
    }

  }


  def _explainParams(sparkSession: SparkSession, f: () => Params, isGroup: Boolean = true) = {

    val rfcParams2 = this.params.map(this.paramToJSon(_, this)).map(f => Row.fromSeq(f))
    val model = f()
    val rfcParams = model.params.map(this.buildInAlgParamToJson(_, model)).map { f =>
      isGroup match {
        case true=>Row.fromSeq(Seq("fitParam.[group]." + f(0), f(1), f(2), f(3)))
        case _=>Row.fromSeq(Seq(f(0), f(1), f(2), f(3)))
      }
    }
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rfcParams2 ++ rfcParams, 1),
      StructType(
        Seq(
          StructField("param", StringType),
          StructField("description", StringType),
          StructField("value", StringType),
          StructField("extra", StringType)
        )))
  }

  def _explainTagParams(sparkSession: SparkSession, f: () => Map[TagParamName, Params]) = {

    val rfcParams2 = this.params.map(this.paramToJSon(_, this)).map(f => Row.fromSeq(f))
    val models = f()
    val rfcParams = models.toList.flatMap { case (tag, model) =>
      model.params.map(this.buildInAlgParamToJson(_, model)).map { f =>
        Row.fromSeq(Seq(s"fitParam.[tag__${tag.field}__${tag.tag}]." + f(0), f(1), f(2), f(3)))
      }
    }
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rfcParams2 ++ rfcParams, 1),
      StructType(
        Seq(
          StructField("param", StringType),
          StructField("description", StringType),
          StructField("value", StringType),
          StructField("extra", StringType)
        )))
  }

  def _explainParams(sparkSession: SparkSession) = {

    val rfcParams2 = this.params.map(this.paramToJSon(_, this)).map(f => Row.fromSeq(f))
    sparkSession.createDataFrame(sparkSession.sparkContext.parallelize(rfcParams2, 1),
      StructType(Seq(
        StructField("param", StringType),
        StructField("description", StringType),
        StructField("value", StringType),
        StructField("extra", StringType)
      )))
  }

  def fetchParam[T](params: Map[String, String], param: Param[T], convert: (String) => T,
                    callback: Param[T] => Unit) = {
    params.get(param.name).map { item =>
      set(param, convert(item))
    }.getOrElse {
      callback(param)
    }
    $(param)
  }

  object ParamDefaultOption {
    def required[T](param: Param[T]): Unit = {
      throw new MLSQLException(s"${param.name} is required")
    }
  }

  object ParamConvertOption {
    def toInt(a: String): Int = {
      a.toInt
    }

    def nothing(a: String) = a
  }


}

object WowParams {
  def randomUID() = {
    Identifiable.randomUID(this.getClass.getName)
  }
}

