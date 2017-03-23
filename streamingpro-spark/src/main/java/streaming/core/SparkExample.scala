package streaming.core

import org.apache.spark.sql.SQLContext
import org.apache.spark.util.{ScalaSourceCodeCompiler, ScriptCacheKey}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by allwefantasy on 22/3/2017.
  */
object SparkExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[2]")
    conf.setAppName("jack")
    val sc = new SparkContext(conf)
    val context = new SQLContext(sc)
    context.read.format("com.databricks.spark.csv").load("file:///tmp/sample.csv").registerTempTable("doctorIndex")

    val script = " import org.apache.spark.sql.types.{LongType, StructField, StructType}\n\n    val context = aContext.asInstanceOf[org.apache.spark.sql.SQLContext]\n    val inputTableName = \"doctorIndex\"\n    val outputTableName = \"doctorIndex2\"\n    val rankField = \"rank\"\n\n    val table = context.table(inputTableName)\n    val schema = table.schema\n    val rdd = table.rdd\n    val newSchema = new StructType(schema.fields ++ Array(StructField(rankField, LongType)))\n\n    val newRowsWithScore = rdd.zipWithIndex().map { f =>\n      org.apache.spark.sql.Row.fromSeq(f._1.toSeq ++ Array(f._2))\n    }\n\n    context.createDataFrame(newRowsWithScore, newSchema).registerTempTable(outputTableName)"

    val abcc = ScalaSourceCodeCompiler.compileAndRun(script, Map("aContext" -> context))
    context.sql("select * from doctorIndex2").show(1000)
    sc.stop()
  }
}

class Rank(context: SQLContext) {

  def rank = {

    import org.apache.spark.sql.types.{LongType, StructField, StructType}

    //val context = aContext.asInstanceOf[org.apache.spark.sql.SQLContext]
    val inputTableName = "doctorIndex"
    val outputTableName = "doctorIndex2"
    val rankField = "rank"

    val table = context.table(inputTableName)
    val schema = table.schema
    val rdd = table.rdd
    val newSchema = new StructType(schema.fields ++ Array(StructField(rankField, LongType)))

    val newRowsWithScore = rdd.zipWithIndex().map { f =>
      org.apache.spark.sql.Row.fromSeq(f._1.toSeq ++ Array(f._2))
    }

    context.createDataFrame(newRowsWithScore, newSchema).registerTempTable(outputTableName)
  }

}



