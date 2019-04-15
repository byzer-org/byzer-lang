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

package streaming.dsl.mmlib.algs

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import streaming.dsl.mmlib.SQLAlg


import scala.collection.JavaConversions._
import scala.collection.mutable

/**
  * Created by allwefantasy on 24/4/2018.
  */
class SQLTokenAnalysis extends SQLAlg with Functions {

  def internal_train(df: DataFrame, params: Map[String, String]) = {
    val session = df.sparkSession

    require(params.contains("inputCol"), "inputCol is required")
    val fieldName = params("inputCol")
    val split = params.getOrElse("split", null)
    val arrayWords = {
      if (params.contains("wordsArray")) {
        params("wordsArray").split(",")
      } else
        Array[String]()
    }

    val words = SQLTokenAnalysis.loadDics(session, params) ++ arrayWords


    val rdd = df.rdd.mapPartitions { mp =>

      val parser = SQLTokenAnalysis.createAnalyzer(words, params)
      mp.map { f =>
        val content = f.getAs[String](fieldName)
        val res = {
          if (split != null) {
            content.split(split)
          }
          else SQLTokenAnalysis.parseStr(parser, content, params)
        }
        val index = f.fieldIndex(fieldName)
        val newValue = f.toSeq.zipWithIndex.filterNot(f => f._2 == index).map(f => f._1) ++ Seq(res)
        Row.fromSeq(newValue)
      }
    }
    session.createDataFrame(rdd,
      StructType(df.schema.filterNot(f => f.name == fieldName) ++ Seq(StructField(fieldName, ArrayType(StringType)))))

  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    val newDf = internal_train(df, params)
    val fieldName = params("inputCol")
    val id = params("idCol")
    newDf.select(F.col(fieldName).alias("keywords"), F.col(id)).write.mode(SaveMode.Overwrite).parquet(path)
    emptyDataFrame()(df)
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    null
  }
}

object SQLTokenAnalysis {
  def parseStr(parser: Any, content: String, params: Map[String, String]) = {

    val ignoreNature = params.getOrElse("ignoreNature", "true").toBoolean
    val filterNatures = params.getOrElse("filterNatures", "").split(",").filterNot(f => f.isEmpty).toSet
    val deduplicateResult = params.getOrElse("deduplicateResult", "false").toBoolean

    val udg = try {
      parser.getClass.getMethod("parseStr", classOf[String]).invoke(parser, content)
    } catch {
      case e: Exception =>
        println(s"parser invoke error:${content}")
        throw e
    }

    def getAllWords(udg: Any) = {
      val result = udg.getClass.getMethod("getTerms").invoke(udg).asInstanceOf[java.util.List[Object]]
      var res = result.map { f =>
        val (name, nature) = AnsjFunctions.getTerm(f)
        (name.toString, nature.toString)
      }

      if (deduplicateResult) {
        val tmpSet = new mutable.HashSet[(String, String)]()
        res.map { f =>
          if (!tmpSet.contains(f)) {
            tmpSet.add(f)
          }
          tmpSet.contains(f)
        }
        res = tmpSet.toBuffer
      }

      if (filterNatures.size > 0) {
        res = res.filter(f => filterNatures.contains(f._2))
      }

      if (ignoreNature) {
        res.map(f => s"${f._1}")
      } else {
        res.map(f => s"${f._1}/${f._2}")
      }
    }

    getAllWords(udg).toArray
  }

  def loadDics(spark: SparkSession, params: Map[String, String]) = {
    var result = Array[String]()
    result ++= params.getOrElse("dic.paths", "").split(",").filter(f => !f.isEmpty).map { f =>
      val wordsList = spark.sparkContext.textFile(f).collect()
      wordsList
    }.flatMap(f => f)
    result
  }


  def createForest(words: Array[String], params: Map[String, String]) = {

    val forestClassName = params.getOrElse("forest", "org.nlpcn.commons.lang.tire.domain.Forest")
    val forest = Class.forName(forestClassName).newInstance().asInstanceOf[AnyRef]
    words.foreach { f =>
      AnsjFunctions.addWord(f, forest)
    }
    forest
  }

  def createAnalyzerFromForest(forest: AnyRef, params: Map[String, String]) = {
    val parserClassName = params.getOrElse("parser", "org.ansj.splitWord.analysis.NlpAnalysis")
    val forestClassName = params.getOrElse("forest", "org.nlpcn.commons.lang.tire.domain.Forest")
    val parser = Class.forName(parserClassName).newInstance().asInstanceOf[AnyRef]
    AnsjFunctions.configureDic(parser, forest, parserClassName, forestClassName)
    parser
  }

  def createAnalyzer(words: Array[String], params: Map[String, String]) = {

    val parserClassName = params.getOrElse("parser", "org.ansj.splitWord.analysis.NlpAnalysis")
    val forestClassName = params.getOrElse("forest", "org.nlpcn.commons.lang.tire.domain.Forest")

    val forest = Class.forName(forestClassName).newInstance().asInstanceOf[AnyRef]
    val parser = Class.forName(parserClassName).newInstance().asInstanceOf[AnyRef]

    words.foreach { f =>
      AnsjFunctions.addWord(f, forest)
    }

    AnsjFunctions.configureDic(parser, forest, parserClassName, forestClassName)
    parser
  }
}


