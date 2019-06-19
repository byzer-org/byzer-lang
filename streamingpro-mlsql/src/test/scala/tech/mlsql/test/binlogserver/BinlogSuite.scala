package tech.mlsql.test.binlogserver

import java.io.File
import java.sql.{SQLException, Statement}
import java.util.TimeZone

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.delta.sources.mysql.binlog._
import org.apache.spark.sql.execution.streaming.{LongOffset, SerializedOffset}
import org.apache.spark.sql.streaming.{OutputMode, StreamTest}
import org.scalatest.time.SpanSugar._

/**
  * 2019-06-15 WilliamZhu(allwefantasy@gmail.com)
  */

trait BaseBinlogTest extends StreamTest {

  override val streamingTimeout = 1800.seconds

  def withTempDirs(f: (File, File) => Unit): Unit = {
    withTempDir { file1 =>
      withTempDir { file2 =>
        f(file1, file2)
      }
    }
  }

  val parameters = Map(
    "host" -> "127.0.0.1",
    "port" -> "3306",
    "userName" -> "root",
    "password" -> "mlsql"
  )

  val bingLogHost = parameters("host")
  val bingLogPort = parameters("port").toInt
  val bingLogUserName = parameters("userName")
  val bingLogPassword = parameters("password")

  var master: MySQLConnection = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"))
    master = new MySQLConnection(bingLogHost, bingLogPort, bingLogUserName, bingLogPassword)
    master.execute(new MySQLConnection.Callback[Statement]() {
      @throws[SQLException]
      override def execute(statement: Statement): Unit = {
        statement.execute("drop database if exists mbcj_test")
        statement.execute("create database mbcj_test")
        statement.execute("use mbcj_test")
      }
    })

    master.execute(new MySQLConnection.Callback[Statement]() {
      @throws[SQLException]
      override def execute(statement: Statement): Unit = {
        statement.execute("drop table if exists script_file")
        statement.execute(
          """
            |CREATE TABLE `script_file` (
            |  `id` int(11) NOT NULL AUTO_INCREMENT,
            |  `name` varchar(255) DEFAULT NULL,
            |  `has_caret` int(11) DEFAULT NULL,
            |  PRIMARY KEY (`id`),
            |  KEY `name` (`name`)
            |) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8
          """.stripMargin)
        statement.execute(
          """
            |insert into script_file (name,has_caret) values ("jack",1)
          """.stripMargin)
      }
    })


  }

  override def afterAll(): Unit = {
    super.afterAll()
    master.close()
  }

  val delta = "org.apache.spark.sql.delta.sources.MLSQLDeltaDataSource"

  def fullSyncMySQLToDelta(path: String) = {
    spark.read.format("jdbc").options(parameters)
      .option("url", s"jdbc:mysql://${bingLogHost}:${bingLogPort}/mbcj_test?characterEncoding=utf8")
      .option("driver", s"com.mysql.jdbc.Driver")
      .option("dbtable", s"script_file").load().write.format(delta).mode(SaveMode.Overwrite).save(path)
  }

}

class BinlogSuite extends BaseBinlogTest with BinLogSocketServerSerDer {

  test("read binlog and write original log to delta table") {
    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>

        val df = spark.readStream.format("org.apache.spark.sql.delta.sources.MLSQLBinLogDataSource")
          .options(parameters)
          .option("metadataPath", s"${checkpointDir.getCanonicalPath}/binlog-offsets")
          .option("databaseNamePattern", "mbcj_test")
          .option("tableNamePattern", "script_file")
          .load()
        val query = df.writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .outputMode(OutputMode.Append())
          .format(delta)
          .start(outputDir.getCanonicalPath)

        try {
          master.execute(new MySQLConnection.Callback[Statement]() {
            @throws[SQLException]
            override def execute(statement: Statement): Unit = {
              statement.execute(
                """
                  |insert into script_file (name,has_caret) values ("jack2",1)
                """.stripMargin)
            }
          })
          query.processAllAvailable()
          val startOffset = LongOffset(SerializedOffset(query.lastProgress.sources.head.startOffset))
          val endOffset = LongOffset(SerializedOffset(query.lastProgress.sources.head.endOffset))
          

          spark.read.format(delta).load(outputDir.getCanonicalPath).createOrReplaceTempView("table1")
          val table1 = spark.sql(""" select * from table1""")
          assert(table1.count() == 1)

        } finally {
          query.stop()
        }
      }
    }
  }


}
