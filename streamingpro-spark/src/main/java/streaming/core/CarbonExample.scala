package streaming.core

import java.io.File

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{CarbonContext, SaveMode}

/**
  * Created by allwefantasy on 23/3/2017.
  */
object CarbonExample {
  def main(args: Array[String]):Unit = {
    val cc = ExampleUtils.createCarbonContext("CarbonExample")


    // Specify date format based on raw data
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, "yyyy/MM/dd")

    cc.sql("""
             CREATE TABLE IF NOT EXISTS t3
                        (ID Int, date Timestamp, country String,
                        name String, phonetype String, serialname String, salary Int,
                        name1 String, name2 String, name3 String, name4 String, name5 String, name6 String,name7 String,name8 String
                        ) STORED BY 'carbondata'
           """)

    cc.sql("desc t3").show(1000,false)



    // Drop table
    //cc.sql("DROP TABLE IF EXISTS t3")
  }
}

object ExampleUtils {

  def currentPath: String = new File(this.getClass.getResource("/").getPath + "../../")
    .getCanonicalPath
  val storeLocation = currentPath + "/target/store"

  def createCarbonContext(appName: String): CarbonContext = {
    val sc = new SparkContext(new SparkConf()
      .setAppName(appName)
      .setMaster("local[2]"))
    sc.setLogLevel("ERROR")

    println(s"Starting $appName using spark version ${sc.version}")

    val cc = new CarbonContext(sc, storeLocation, currentPath + "/target/carbonmetastore")

    CarbonProperties.getInstance()
      .addProperty("carbon.storelocation", storeLocation)
    // whether use table split partition
    // true -> use table split partition, support multiple partition loading
    // false -> use node split partition, support data load by host partition
    CarbonProperties.getInstance().addProperty("carbon.table.split.partition.enable", "false")
    cc
  }

  /**
    * This func will write a sample CarbonData file containing following schema:
    * c1: String, c2: String, c3: Double
    * Returns table path
    */
  def writeSampleCarbonFile(cc: CarbonContext, tableName: String, numRows: Int = 1000): String = {
    cc.sql(s"DROP TABLE IF EXISTS $tableName")
    writeDataframe(cc, tableName, numRows, SaveMode.Overwrite)
    s"$storeLocation/default/$tableName"
  }

  /**
    * This func will append data to the CarbonData file
    * Returns table path
    */
  def appendSampleCarbonFile(cc: CarbonContext, tableName: String, numRows: Int = 1000): String = {
    writeDataframe(cc, tableName, numRows, SaveMode.Append)
    s"$storeLocation/default/$tableName"
  }

  /**
    * create a new dataframe and write to CarbonData file, based on save mode
    */
  private def writeDataframe(
                              cc: CarbonContext, tableName: String, numRows: Int, mode: SaveMode): Unit = {
    // use CarbonContext to write CarbonData files
    import cc.implicits._
    val sc = cc.sparkContext
    val df = sc.parallelize(1 to numRows, 2)
      .map(x => ("a", "b", x))
      .toDF("c1", "c2", "c3")

    // save dataframe directl to carbon file without tempCSV
    df.write
      .format("carbondata")
      .option("tableName", tableName)
      .option("compress", "true")
      .option("use_kettle", "false")
      .option("tempCSV", "false")
      .mode(mode)
      .save()
  }

  def cleanSampleCarbonFile(cc: CarbonContext, tableName: String): Unit = {
    cc.sql(s"DROP TABLE IF EXISTS $tableName")
  }
}
