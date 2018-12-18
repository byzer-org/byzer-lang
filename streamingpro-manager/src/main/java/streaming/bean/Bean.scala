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

package streaming.bean

import net.sf.json.{JSONArray, JSONObject}
import streaming.db.{TSparkJar, TSparkJobParameter}

import scala.collection.JavaConversions._
import scala.io.Source

/**
  * Created by allwefantasy on 14/7/2017.
  */
object DeployParameterService {
  val parameterProcessorMapping = register()
  val parameterFieldMapping = register2()

  def register() = {
    Map(
      "select" -> new ComplexParameterProcessor(),
      "normal" -> new NoneAppParameterProcessor(),
      "checkbox" -> new ComplexParameterProcessor()

    )
  }

  def register2() = {
    Map(
      "spark" -> new SparkParameter(),
      "jar" -> new TSparkJarParameter()
    )
  }

  import net.liftweb.{json => SJSon}

  def parseJson[T](str: String)(implicit m: Manifest[T]) = {
    implicit val formats = SJSon.DefaultFormats
    SJSon.parse(str).extract[T]
  }

  def toStr(obj: AnyRef) = {
    implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
    SJSon.Serialization.write(obj)
  }

  def process() = {
    TSparkJobParameter.list.map {
      f =>
        val appParameterProcessor = parameterProcessorMapping(f.actionType)
        appParameterProcessor.process(f)
    }
  }
}


trait AppParameterProcessor {
  def process(actionType: TSparkJobParameter): TSparkJobParameter
}

class ComplexParameterProcessor extends AppParameterProcessor {
  override def process(actionType: TSparkJobParameter): TSparkJobParameter = {
    if (!actionType.value.isEmpty) return actionType
    if (actionType.app.isEmpty || actionType.app == "-") return actionType
    val v = DeployParameterService.parameterFieldMapping(actionType.app).value(actionType).mkString(",")
    new TSparkJobParameter(actionType.id,
      actionType.name,
      actionType.parentName,
      actionType.parameterType,
      actionType.app,
      actionType.description,
      actionType.label,
      actionType.priority,
      actionType.formType,
      actionType.actionType,
      actionType.actionType,
      v)

  }
}

class NoneAppParameterProcessor extends AppParameterProcessor {
  override def process(item: TSparkJobParameter): TSparkJobParameter = {
    item
  }
}

class SparkParameter extends BuildSelectParameterValues {
  override def value(actionType: TSparkJobParameter): List[String] = {
    val str = Source.fromInputStream(this.getClass.getResourceAsStream("/sparkParameter.json")).getLines().mkString("\n")
    import scala.collection.JavaConversions._
    val keyValue = JSONObject.fromObject(str).map {
      f =>
        (f._1.asInstanceOf[String], f._2.asInstanceOf[JSONArray].map(f => f.asInstanceOf[String]).toList)
    }.toMap
    keyValue.getOrElse(actionType.name, List())
  }
}

class TSparkJarParameter extends BuildSelectParameterValues {
  override def value(actionType: TSparkJobParameter): List[String] = {
    val jrs = TSparkJar.list
    jrs.map(f => f.name + ":" + f.path)
  }
}

trait BuildSelectParameterValues {
  def value(actionType: TSparkJobParameter): List[String]
}