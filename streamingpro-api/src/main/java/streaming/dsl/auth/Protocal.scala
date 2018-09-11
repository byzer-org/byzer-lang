package streaming.dsl.auth

/**
  * Created by allwefantasy on 11/9/2018.
  */
case class MLSQLTable(db: Option[String], table: Option[String], tableType: TableTypeMeta)

case class MLSQLTableSet(tables: Seq[MLSQLTable])

case class TableTypeMeta(name: String, includes: Set[String])

case class TableAuthResult(granted: Boolean, msg: String)

object TableAuthResult {
  def empty() = {
    TableAuthResult(false, "")
  }
}

object TableType {
  val HIVE = TableTypeMeta("hive", Set("hive"))
  val HBASE = TableTypeMeta("hbase", Set("hbase"))
  val HDFS = TableTypeMeta("hdfs", Set("parquet", "json", "csv"))
  val HTTP = TableTypeMeta("hdfs", Set("http"))
  val JDBC = TableTypeMeta("jdbc", Set("jdbc"))
  val ES = TableTypeMeta("es", Set("es"))
  val TEMP = TableTypeMeta("temp", Set("temp"))

  def from(str: String) = {
    List(HIVE, HBASE, HDFS, HTTP, JDBC, ES, TEMP).filter(f => f.includes.contains(str)).headOption
  }
}


