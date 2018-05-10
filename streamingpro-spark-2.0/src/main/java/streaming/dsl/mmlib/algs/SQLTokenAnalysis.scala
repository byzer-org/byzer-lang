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
    var result = Array[String]()
    require(params.contains("inputCol"), "inputCol is required")

    val ignoreNature = params.getOrElse("ignoreNature", "false").toBoolean
    val filterNatures = params.getOrElse("filterNatures", "").split(",").filterNot(f => f.isEmpty).toSet
    val deduplicateResult = params.getOrElse("deduplicateResult", "false").toBoolean

    val fieldName = params("inputCol")
    val parserClassName = params.getOrElse("parser", "org.ansj.splitWord.analysis.NlpAnalysis")
    val forestClassName = params.getOrElse("forest", "org.nlpcn.commons.lang.tire.domain.Forest")

    result ++= params.getOrElse("dic.paths", "").split(",").filter(f => !f.isEmpty).map { f =>
      val wordsList = session.sparkContext.textFile(f).collect()
      wordsList
    }.flatMap(f => f)

    val ber = session.sparkContext.broadcast(result)
    val rdd = df.rdd.mapPartitions { mp =>

      val forest = Class.forName(forestClassName).newInstance().asInstanceOf[AnyRef]
      val parser = Class.forName(parserClassName).newInstance().asInstanceOf[AnyRef]

      ber.value.foreach { f =>
        AnsjFunctions.addWord(f, forest)
      }

      AnsjFunctions.configureDic(parser, forest, parserClassName, forestClassName)
      mp.map { f =>
        val content = f.getAs[String](fieldName)

        val udg = parser.getClass.getMethod("parseStr", classOf[String]).invoke(parser, content)
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

        val index = f.fieldIndex(fieldName)
        val newValue = f.toSeq.zipWithIndex.filterNot(f => f._2 == index).map(f => f._1) ++ Seq(getAllWords(udg).toArray)
        Row.fromSeq(newValue)
      }
    }

    session.createDataFrame(rdd,
      StructType(df.schema.filterNot(f => f.name == fieldName) ++ Seq(StructField(fieldName, ArrayType(StringType)))))

  }

  override def train(df: DataFrame, path: String, params: Map[String, String]): Unit = {
    val newDf = internal_train(df, params)
    val fieldName = params("inputCol")
    val id = params("idCol")
    newDf.select(F.col(fieldName).alias("keywords"), F.col(id)).write.mode(SaveMode.Overwrite).parquet(path)
  }

  override def load(sparkSession: SparkSession, path: String): Any = {
    null
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String): UserDefinedFunction = {
    null
  }
}
