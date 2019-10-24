package tech.mlsql.test

import net.sf.json.JSONObject
import org.apache.spark.sql.catalyst.parser.LegacyTypeStringParser
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{SparkSession, functions => F}
import streaming.source.parser.impl.JsonSourceParser

import scala.collection.JavaConverters._
import scala.util.Try

/**
 * 24/10/2019 WilliamZhu(allwefantasy@gmail.com)
 */
object WowTest {


  def main(args: Array[String]): Unit = {
    def deserializeSchema(json: String): StructType = {
      Try(DataType.fromJson(json)).getOrElse(LegacyTypeStringParser.parse(json)) match {
        case t: StructType => t
        case _ => throw new RuntimeException(s"Failed parsing StructType: $json")
      }
    }

    val schema = JSONObject.fromObject(value).getString("schema")
    val sourceSchema = deserializeSchema(schema)
    val sourceParserInstance = new JsonSourceParser()
    val spark = SparkSession.builder().appName("wow").master("local[*]").getOrCreate();
    val tempRDD = spark.sparkContext.
      parallelize[String](JSONObject.fromObject(value).getJSONArray("rows").
        asScala.map(f => f.toString))
    tempRDD.collect().map(f => println(f))
    println(sourceSchema)
    import spark.implicits._
    val df = spark.createDataset[String](tempRDD).toDF("value").select(sourceParserInstance.parseRaw(F.col("value"), sourceSchema, Map()).as("data"))
      .select("data.*")
    df.show()
  }

  val value =
    """
      |{"type":"insert","timestamp":1571906499000,"databaseName":"jute","tableName":"jute_post","schema":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"userId\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"username\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"subject\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"boardId\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"topicId\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"parent\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"parentUsername\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"root\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"child\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"length\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"click\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"newOrder\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"sorter\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"notify\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"iP\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"realIP\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"postTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"file\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"editTime\",\"type\":\"timestamp\",\"nullable\":true,\"metadata\":{}},{\"name\":\"editedBy\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"deleted\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"markup\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"smiley\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"signatured\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"rating\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"ratedBy\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},{\"name\":\"semiHidden\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"value\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"type\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"votes\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"fileFree\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"floor\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}},{\"name\":\"mask\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}},{\"name\":\"postRewardMoney\",\"type\":\"integer\",\"nullable\":true,\"metadata\":{}}]}","rows":[{"id":42156022,"userId":59430839,"username":"dxy_o35xoos7","subject":"回复：电子化注册显示准考证号错误","boardId":82,"topicId":9,"parent":42100485,"parentUsername":"子傲9101","root":42100485,"child":0,"length":29,"click":0,"newOrder":0,"sorter":"0.14","notify":0,"iP":"223.104.65.161","realIP":"223.104.65.161","postTime":1571935299000,"file":"{}","deleted":0,"markup":1,"smiley":1,"signatured":0,"rating":0,"semiHidden":0,"value":0,"type":400,"votes":0,"fileFree":1,"floor":16,"mask":0,"postRewardMoney":0}]}
      |""".stripMargin


}
