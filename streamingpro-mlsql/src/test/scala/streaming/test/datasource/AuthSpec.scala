package streaming.test.datasource

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.OperateType
import streaming.dsl.auth.meta.client.DefaultConsoleClient
import streaming.log.Logging
import streaming.test.datasource.help.MLSQLTableEnhancer._

/**
  * 2019-03-20 WilliamZhu(allwefantasy@gmail.com)
  */
class AuthSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig with BeforeAndAfterAll with Logging {

  def executeScript(script: String)(implicit runtime: SparkRuntime) = {
    implicit val spark = runtime.sparkSession
    val ssel = createSSEL
    ScriptSQLExec.parse(script, ssel, true, false, true)
  }

  "kafka auth" should "[kafka] work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      val spark = runtime.sparkSession

      executeScript(
        """
          |set streamName="test";
          |load kafka.`topic_name` options
          |`kafka.bootstrap.servers`="---"
          |as kafka_post_parquet;
          |
        """.stripMargin)
      val tables = DefaultConsoleClient.get

      val kafkaTable = tables.filter(f => f.tableType.name == "kafka" && f.operateType == OperateType.LOAD).head
      assert(kafkaTable.table.get == "topic_name")

      val tempTable = tables.filter(f => f.tableType.name == "temp").head
      assert(tempTable.table.get == "kafka_post_parquet")

    }
  }


  "unknow auth" should "[unknow] work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      val spark = runtime.sparkSession

      executeScript(
        """
          |set streamName="test";
          |load jack.`topic_name` options
          |`kafka.bootstrap.servers`="---"
          |as kafka_post_parquet;
          |
        """.stripMargin)
      val tables = DefaultConsoleClient.get

      val unknowTable = tables.filter(f => f.tableType.name == "unknow" && f.operateType == OperateType.LOAD).head
      assert(unknowTable.table.get == "topic_name")

      val tempTable = tables.filter(f => f.tableType.name == "temp").head
      assert(tempTable.table.get == "kafka_post_parquet")

    }
  }

  "mysql auth" should "[mysql] work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      val spark = runtime.sparkSession

      executeScript(
        """
          |connect jdbc where
          |driver="com.mysql.jdbc.Driver"
          |and url="jdbc:mysql://${mysql_pi_search_ip}:${mysql_pi_search_port}/white_db?${MYSQL_URL_PARAMS}"
          |and user="${mysql_pi_search_user}"
          |and password="${mysql_pi_search_password}"
          |as white_db_ref;
          |
          |load jdbc.`white_db_ref.people`
          |as people;
          |
          |save append people as jdbc.`white_db_ref.spam_inf` ;
          |
        """.stripMargin)
      val tables = DefaultConsoleClient.get
      assert(tables.size == 3)
      // one is white_db.people
      // the other is temp table people
      // and save operate visit

      val jdbcTable = tables.filter(f => f.isSourceTypeOf("mysql") && f.operateType == OperateType.LOAD).head
      assert(jdbcTable.db.get == "white_db")
      assert(jdbcTable.table.get == "people")

      val tempTable = tables.filter(f => f.tableType.name == "temp").head
      assert(tempTable.table.get == "people")

      val saveTable = tables.filter(f => f.isSourceTypeOf("mysql") && f.operateType == OperateType.SAVE).head
      assert(saveTable.db.get == "white_db")
      assert(saveTable.table.get == "spam_inf")

    }
  }


  "hive auth" should "[hive] work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      val spark = runtime.sparkSession

      executeScript(
        """
          |select * from jack.dd as table1;
        """.stripMargin)
      var tables = DefaultConsoleClient.get

      var hiveTable = tables.filter(f => f.tableType.name == "hive" && f.operateType == OperateType.SELECT).head
      assert(hiveTable.db.get == "jack")
      assert(hiveTable.table.get == "dd")

      var tempTable = tables.filter(f => f.tableType.name == "temp").head
      assert(tempTable.table.get == "table1")


      executeScript(
        """
          |load hive.`jack` as k;
          |select * from k as table1;
        """.stripMargin)
      tables = DefaultConsoleClient.get

      hiveTable = tables.filter(f => f.tableType.name == "hive" && f.operateType == OperateType.LOAD).head
      assert(hiveTable.db.get == "default")
      assert(hiveTable.table.get == "jack")

      assert(tables.filter(f => f.tableType.name == "temp").size == 2)


    }
  }
}
