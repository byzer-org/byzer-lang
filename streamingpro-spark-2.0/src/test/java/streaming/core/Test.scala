package streaming.core

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by allwefantasy on 28/3/2017.
  */
object Test {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BatchSparkSQL")
    conf.setMaster("local[2]")
    val sc = SparkSession.builder().config(conf).getOrCreate()

    val words = sc.sparkContext.textFile("file:///Users/allwefantasy/Downloads/results-3.csv")

    val input = words.mapPartitions { f =>
      val jack = new Jack()
      f.map(k => jack.echo(k))
    }
    input.count()
    sc.stop()

  }
}
