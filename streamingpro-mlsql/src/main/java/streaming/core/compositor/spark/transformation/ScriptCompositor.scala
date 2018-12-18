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

package streaming.core.compositor.spark.transformation

import java.util

import net.liftweb.{json => SJSon}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.{ScalaSourceCodeCompiler, ScriptCacheKey}

import scala.collection.JavaConversions._

/**
  * 8/2/16 WilliamZhu(allwefantasy@gmail.com)
  */
class ScriptCompositor[T] extends BaseDFCompositor[T] {


  def scripts = {
    _configParams.get(1).map { fieldAndCode =>
      (fieldAndCode._1.toString, fieldAndCode._2 match {
        case a: util.List[String] => a.mkString(" ")
        case a: String => a
        case _ => ""
      })
    }
  }

  def schema = {
    config[String]("schema", _configParams)
  }

  def script = {
    config[String]("script", _configParams)
  }


  val LANGSET = Set("scala", "python")


  def source = {
    config[String]("source", _configParams)
  }

  def lang = {
    config[String]("lang", _configParams)
  }

  def useDocMap = {
    config[Boolean]("useDocMap", _configParams)
  }

  def ignoreOldColumns = {
    config[Boolean]("ignoreOldColumns", _configParams)
  }

  def samplingRatio = {
    config[Double]("samplingRatio", _configParams)
  }


  def createDF(params: util.Map[Any, Any], df: DataFrame) = {
    val _script = script.getOrElse("")
    val _schema = schema.getOrElse("")
    val _source = source.getOrElse("")
    val _useDocMap = useDocMap.getOrElse(false)
    val _ignoreOldColumns = ignoreOldColumns.getOrElse(false)
    val _samplingRatio = samplingRatio.getOrElse(1.0)


    def loadScriptFromFile(script: String) = {
      if ("file" == _source || script.startsWith("file:/") || script.startsWith("hdfs:/")) {
        df.sqlContext.sparkContext.textFile(script).collect().mkString("\n")
      } else if (script.startsWith("classpath:/")) {
        val cleanScriptFilePath = script.substring("classpath://".length)
        scala.io.Source.fromInputStream(
          this.getClass.getResourceAsStream(cleanScriptFilePath)).getLines().
          mkString("\n")
      }
      else script
    }


    val _maps = if (_script.isEmpty) {
      scripts.map { fieldAndCode =>
        (fieldAndCode._1, loadScriptFromFile(fieldAndCode._2))
      }
    } else {
      Map("anykey" -> _script)
    }




    val rawRdd = df.rdd.map { row =>
      val maps = _maps.flatMap { f =>
        val executor = ScalaSourceCodeCompiler.execute(ScriptCacheKey(if (_useDocMap) "doc" else "rawLine", f._2))
        if (_useDocMap) {
          executor.execute(row.getValuesMap(row.schema.fieldNames))
        } else {
          executor.execute(row.getAs[String](f._1))
        }
      }

      val newMaps = if (_ignoreOldColumns) maps
      else row.schema.fieldNames.map(f => (f, row.getAs(f))).toMap ++ maps

      newMaps
    }



    if (schema != None) {

      val schemaScript = loadScriptFromFile(_schema)
      val rowRdd = rawRdd.map { newMaps =>
        val schemaExecutor = ScalaSourceCodeCompiler.execute(ScriptCacheKey("schema", schemaScript))
        val st = schemaExecutor.schema().get
        Row.fromSeq(st.fieldNames.map { fn =>
          if (newMaps.containsKey(fn)) newMaps(fn) else null
        }.toSeq)
      }
      val schemaExecutor = ScalaSourceCodeCompiler.execute(ScriptCacheKey("schema", schemaScript))
      df.sqlContext.createDataFrame(rowRdd, schemaExecutor.schema().get)
    } else {

      val jsonRdd = rawRdd.map { newMaps =>
        implicit val formats = SJSon.Serialization.formats(SJSon.NoTypeHints)
        SJSon.Serialization.write(newMaps)
      }
      df.sqlContext.read.option("samplingRatio", _samplingRatio + "").json(jsonRdd)
    }


  }

}
