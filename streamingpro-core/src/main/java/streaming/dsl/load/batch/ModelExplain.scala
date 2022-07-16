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

package streaming.dsl.load.batch

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import tech.mlsql.dsl.adaptor.MLMapping
import tech.mlsql.tool.HDFSOperatorV2


/**
  * Created by allwefantasy on 21/9/2018.
  */
object ModelSelfExplain {
  def apply(format: String, path: String, option: Map[String, String], sparkSession: SparkSession): ModelSelfExplain = new ModelSelfExplain(format, path, option)(sparkSession)

  def findAlg(name: String) = {
    try {
      Some(MLMapping.findAlg(name))
    } catch {
      case e: Exception => None
    }

  }
}

class ModelSelfExplain(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) {

  private var table: DataFrame = null


  def isMatch = {
    new Then(this, Set("model", "modelList", "modelParams", "modelExample", "modelExplain").contains(format))
  }

  def explain() = {
    table = List(
      new ModelStartup(format, path, option)(sparkSession),
      new ModelList(format, path, option)(sparkSession),
      new ModelParams(format, path, option)(sparkSession),
      new ModelExample(format, path, option)(sparkSession),
      new ModelExplain(format, path, option)(sparkSession)
    ).
      filter(explainer => explainer.isMatch).
      head.
      explain
  }

  class Then(parent: ModelSelfExplain, isMatch: Boolean) {
    def thenDo() = {
      if (isMatch) {
        parent.explain()
      }
      new OrElse(parent, isMatch)
    }

  }

  class OrElse(parent: ModelSelfExplain, isMatch: Boolean) {
    def orElse(f: () => DataFrame) = {
      if (!isMatch) {
        parent.table = f()
      }
      parent
    }

  }

  def get() = {
    table
  }

}

class ModelStartup(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    format == "model" && path.isEmpty
  }

  override def explain: DataFrame = {

    val rows = sparkSession.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq("How to use algorithm or data process module in MLSQL", "('train'|'TRAIN') tableName 'as' format '.' path 'where'? expression? booleanExpression*  \n check document: https://github.com/allwefantasy/streamingpro/blob/master/docs/en/mlsql-grammar.md#train")),
      Row.fromSeq(Seq("List available algorithm or data process module", "load model.`list` as output;")),
      Row.fromSeq(Seq("Explain params of specific algorithm or data process modules", """load model.`params` where alg="RandomForest" as output; """))
    ), 1)

    sparkSession.createDataFrame(rows,
      StructType(Seq(
        StructField(name = "desc", dataType = StringType),
        StructField(name = "command", dataType = StringType)
      )))
  }
}

class ModelList(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    (format == "model" && path == "list") || (format == "modelList")
  }

  override def explain: DataFrame = {

    val items = MLMapping.getAllETNames.toSeq.sorted.map{algName=>
      val sqlAlg = ModelSelfExplain.findAlg(algName).get
      Row.fromSeq(Seq(algName, sqlAlg.modelType.humanFriendlyName,
        sqlAlg.coreCompatibility.map(f => f.coreVersion).mkString(","),
        sqlAlg.doc.doc, sqlAlg.doc.docType.docType
      )
      )
    }
    
    val rows = sparkSession.sparkContext.parallelize(items, 1)
    sparkSession.createDataFrame(rows,
      StructType(Seq(
        StructField(name = "name", dataType = StringType),
        StructField(name = "algType", dataType = StringType),
        StructField(name = "sparkCompatibility", dataType = StringType),
        StructField(name = "doc", dataType = StringType),
        StructField(name = "docType", dataType = StringType)
      )))
  }
}

class ModelParams(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    (format == "model" && path == "params") ||
      (format == "modelParams")
  }

  private def getAlg = {
    if (format == "model" && path == "params")
      option("alg")
    else {
      path
    }
  }


  override def explain: DataFrame = {
    ModelSelfExplain.findAlg(getAlg) match {
      case Some(i) => i.explainParams(sparkSession)
      case None =>
        import sparkSession.implicits._
        Seq.empty[(String, String)].toDF("param", "description")
    }
  }
}


class ModelExample(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    (format == "model" && path == "example") ||
      (format == "modelExample")
  }

  private def getAlg = {
    if (format == "model" && path == "example")
      option("alg")
    else {
      path
    }
  }


  override def explain: DataFrame = {
    val alg = MLMapping.findAlg(getAlg)
    alg.codeExample

    val rows = sparkSession.sparkContext.parallelize(Seq(
      Row.fromSeq(Seq("name", getAlg)),
      Row.fromSeq(Seq("codeExample", alg.codeExample.code)),
      Row.fromSeq(Seq("codeType", alg.codeExample.codeType.codeType)),
      Row.fromSeq(Seq("doc", alg.doc.doc)),
      Row.fromSeq(Seq("docType", alg.doc.docType.docType))
    ), 1)

    sparkSession.createDataFrame(rows,
      StructType(Seq(
        StructField(name = "name", dataType = StringType),
        StructField(name = "value", dataType = StringType)
      )))


  }
}

class ModelExplain(format: String, path: String, option: Map[String, String])(sparkSession: SparkSession) extends SelfExplain {
  override def isMatch: Boolean = {
    (format == "model" && path == "explain") ||
      (format == "modelExplain")
  }

  private def getAlg = {
    if ((format == "model" && path == "explain") || format == "modelExplain")
      option("alg")
    else {
      path
    }
  }

  private def getPath = {
    if ((format == "model" && path == "explain"))
      option("path")
    else {
      option.get("index") match {
          // If user specifies index, try to find _model_n subdirectory
        case Some(idx) =>
          val subDirs = HDFSOperatorV2.listFiles(path)
              .filter( _.isDirectory )
          val _modelDirExists = subDirs.exists( _.getPath.getName.startsWith("_model_"))
          val _modelIdxDirExists = subDirs.exists( s"_model_${idx}" == _.getPath.getName)
          val metaDirExists =  subDirs.exists( "meta" == _.getPath.getName)
          val modelDirExists = subDirs.exists( "model" == _.getPath.getName)
          if( _modelDirExists && _modelIdxDirExists) {
            s"${path}/_model_${idx}"
          }
          else if( _modelDirExists && ! _modelIdxDirExists ) {
            throw new RuntimeException(s"model directory with index ${idx} does not exist")
          }
          else if( metaDirExists && modelDirExists ) {
            // `keepVersion`="false", index option is ignored.
            path
          }
          else {
            throw new RuntimeException(s"${path}/_model_${idx} does not exist")
          }

          // If keepVersion is not enabled
        case None => path
      }
    }
  }


  override def explain: DataFrame = {
    ModelSelfExplain.findAlg(getAlg) match {
      case Some(alg) =>
        alg.explainModel(sparkSession, getPath, option)
      case None =>
        import sparkSession.implicits._
        Seq.empty[(String, String)].toDF("param", "description")
    }

  }
}
