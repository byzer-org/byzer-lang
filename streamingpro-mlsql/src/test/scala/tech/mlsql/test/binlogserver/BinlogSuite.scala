package tech.mlsql.test.binlogserver

import java.io.File
import java.sql.{SQLException, Statement}
import java.util.TimeZone

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.delta.sources.MLSQLBinLogDataSource
import org.apache.spark.sql.delta.sources.mysql.binlog._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.scalatest.time.SpanSugar._
import tech.mlsql.common.ScalaReflect

/**
  * 2019-06-15 WilliamZhu(allwefantasy@gmail.com)
  */

trait BaseBinlogTest extends WowStreamTest {

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

  object TriggerData {
    def apply(source: Source, f: () => Unit) = {
      new TriggerData(source) {
        override def addData(query: Option[StreamExecution]): (BaseStreamingSource, Offset) = {
          f()
          (source, source.getOffset.get)
        }
      }
    }
  }

  abstract case class TriggerData(source: Source) extends AddData {

  }

  test("read binlog and write original log to delta table") {

    failAfter(streamingTimeout) {
      withTempDirs { (outputDir, checkpointDir) =>

        def addData(sql: String) = {
          master.execute(new MySQLConnection.Callback[Statement]() {
            @throws[SQLException]
            override def execute(statement: Statement): Unit = {
              statement.execute(sql)
            }
          })
        }

        val source = new MLSQLBinLogDataSource().createSource(spark.sqlContext, checkpointDir.getCanonicalPath, Some(StructType(Seq(StructField("value", StringType)))), "binlog", parameters ++ Map(
          "databaseNamePattern" -> "mbcj_test",
          "tableNamePattern" -> "script_file"
        ))
        val attributes = ScalaReflect.fromInstance[StructType](source.schema).method("toAttributes").invoke().asInstanceOf[Seq[AttributeReference]]
        val logicalPlan = StreamingExecutionRelation(source, attributes)(sqlContext.sparkSession)
        val df = toDf(logicalPlan)

        testStream(df, OutputMode.Append())(StartStream(Trigger.ProcessingTime("5 seconds"), new StreamManualClock),
          AdvanceManualClock(5 * 1000),
          TriggerData(source, () => {
            addData(
              """
                |insert into script_file (name,has_caret) values ("jack2",1)
              """.stripMargin)
            // make sure MySQL binlog can be consumed into queue, and the lastest offset will not change again
            // otherwize the CheckNewAnswerRows will block
            //
            Thread.sleep(5 * 1000)
          }),
          AdvanceManualClock(5 * 1000),
          CheckAnswerRowsByFunc(rows => {
            assert(rows.size == 1)
            assert(rows(0).getString(0).contains("jack2"))
          }, true)
        )

      }
    }
  }


}
