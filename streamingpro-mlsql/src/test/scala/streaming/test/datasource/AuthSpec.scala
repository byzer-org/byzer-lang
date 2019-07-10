package streaming.test.datasource

import org.apache.spark.streaming.BasicSparkOperation
import org.scalatest.BeforeAndAfterAll
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec
import streaming.dsl.auth.meta.client.DefaultConsoleClient
import streaming.dsl.auth.{OperateType, TableType}
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

  "mysql variable auth" should "[mysql] work fine" in {

    withBatchContext(setupBatchContext(batchParamsWithoutHive, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      val spark = runtime.sparkSession

      executeScript(
        """
          |set url = "jdbc:mysql://${mysql_pi_search_ip}:${mysql_pi_search_port}/white_db?${MYSQL_URL_PARAMS}";
          |set user = "${mysql_pi_search_user}";
          |set password="${mysql_pi_search_password}";
          |connect jdbc where
          |driver="com.mysql.jdbc.Driver"
          |and url="${url}"
          |and user="${user}"
          |and password="${password}"
          |as white_db_ref;
          |
          |load jdbc.`white_db_ref.people`
          |as people;
          |
          |save append people as jdbc.`white_db_ref.spam_inf` ;
          |
        """.stripMargin)
      val tables = DefaultConsoleClient.get
      assert(tables.size == 6)

      val jdbcTable = tables.filter(f => f.isSourceTypeOf("mysql") && f.operateType == OperateType.LOAD).head
      assert(jdbcTable.db.get == "white_db")
      assert(jdbcTable.table.get == "people")
      assert(jdbcTable.sourceType.get == "mysql")

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

      executeScript(
        """
          |select * from k as tableNew;
        """.stripMargin)
      tables = DefaultConsoleClient.get
      var tempTables = tables.filter(f => f.tableType.name == "hive")
      assert(tempTables.size == 1)


      runtime.sparkSession.sql("select 1 as a ").createOrReplaceTempView("k")
      executeScript(
        """
          |select * from k as tableNew;
        """.stripMargin)
      tables = DefaultConsoleClient.get
      tempTables = tables.filter(f => f.tableType.name == "temp")
      assert(tempTables.size == 2)
      assert(tempTables(0).tableType.name == "temp" && tempTables(0).table.get == "k")
    }
  }


  "auth-mongo" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      //执行sql
      val spark = runtime.sparkSession

      val mlsql =
        """
          |connect mongo where
          |    partitioner="MongoPaginateBySizePartitioner"
          |and uri="mongodb://127.0.0.1:27017/twitter" as mongo_instance;
          |
          |load mongo.`mongo_instance/cool`
          |as test_table;
          |
          |load mongo.`cool_1` where
          |    partitioner="MongoPaginateBySizePartitioner"
          |and uri="mongodb://127.0.0.1:27017/twitter_1"
          |as test_table_2;
          |
          |save overwrite test_table as mongo.`mongo_instance/cool_2` where
          |    partitioner="MongoPaginateBySizePartitioner";
          |
          |load mongo.`cool_10` where
          |    partitioner="MongoPaginateBySizePartitioner"
          |and uri="mongodb://127.0.0.1:27017/twitter_1"
          |and database="twitter_10"
          |as test_table_3;
        """.stripMargin
      executeScript(mlsql)
      var tables = DefaultConsoleClient.get

      val loadMLSQLTable = tables
        .filter(f => (f.tableType == TableType.MONGO && f.operateType == OperateType.LOAD))

      var table = loadMLSQLTable.map(f => f.table.get).toSet
      assume(table == Set("cool", "cool_1", "cool_10"))
      var db = loadMLSQLTable.map(f => f.db.get).toSet
      assume(db == Set("twitter", "twitter_1", "twitter_10"))
      var sourceType = loadMLSQLTable.map(f => f.sourceType.get).toSet
      assume(sourceType == Set("mongo"))

      val saveMLSQLTable = tables
        .filter(f => (f.tableType == TableType.MONGO && f.operateType == OperateType.SAVE))
      table = saveMLSQLTable.map(f => f.table.get).toSet
      assume(table == Set("cool_2"))
      db = saveMLSQLTable.map(f => f.db.get).toSet
      assume(db == Set("twitter"))
      sourceType = saveMLSQLTable.map(f => f.sourceType.get).toSet
      assume(sourceType == Set("mongo"))
    }
  }

  "auth-es" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession


      val mlsql =
        """
          |connect es where
          |`es.nodes`="127.0.0.1"
          |and `es.port`="9200"
          |and `es.resource`="test_index"
          |as es_conn;
          |
          |load es.`es_conn/test_type`
          |as es_test;
          |
          |load es.`test_type_1` options `es.nodes`="127.0.0.1"
          |and `es.resource`="test_index_1"
          |and `es.port`="9200"
          |as es_test_1;
          |
          |load es.`.test.index/` options `es.nodes`="172.16.1.159"
          |and `es.port`="9200"
          |as es_test;
          |
          |save overwrite data1 as es.`es_conn/test_index_2` where
          |`es.index.auto.create`="true"
          |and `es.port`="9200"
          |and `es.nodes`="127.0.0.1";
          |
          |connect es where
          |`es.nodes`="127.0.0.1"
          |and `es.port`="9200"
          |as es_conn1;
          |
          |load es.`es_conn1/index/ttype` options `es.nodes`="172.16.1.159"
          |and `es.port`="9200"
          |as es_test2;
        """.stripMargin

      executeScript(mlsql)

      var tables = DefaultConsoleClient.get

      val loadMLSQLTable = tables
        .filter(f => (f.tableType == TableType.ES && f.operateType == OperateType.LOAD))

      var table = loadMLSQLTable.map(f => f.table.get).toSet
      assume(table == Set("test_type", "test_type_1", "", "ttype"))
      var db = loadMLSQLTable.map(f => f.db.get).toSet
      assume(db == Set("test_index", "test_index_1", ".test.index", "index"))
      var sourceType = loadMLSQLTable.map(f => f.sourceType.get).toSet
      assume(sourceType == Set("es"))

      val saveMLSQLTable = tables
        .filter(f => (f.tableType == TableType.ES && f.operateType == OperateType.SAVE))
      table = saveMLSQLTable.map(f => f.table.get).toSet
      assume(table == Set("test_index_2"))
      db = saveMLSQLTable.map(f => f.db.get).toSet
      assume(db == Set("test_index"))
      sourceType = saveMLSQLTable.map(f => f.sourceType.get).toSet
      assume(sourceType == Set("es"))
    }
  }

  "auth-solr" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val mlsql =
        """
      connect solr where `zkhost`="127.0.0.1:9983"
      and `collection`="mlsql_example"
      and `flatten_multivalued`="false"
      as solr1
      ;

      load solr.`solr1/mlsql_example` as mlsql_example;

      save mlsql_example_data as solr.`solr1/mlsql_example`
      options soft_commit_secs = "1";
      """.stripMargin

      executeScript(mlsql)

      var tables = DefaultConsoleClient.get
      val loadMLSQLTable = tables.filter(f => (f.tableType == TableType.SOLR && f.operateType == OperateType.LOAD))

      var db = loadMLSQLTable.map(f => f.db.get).toSet
      assume(db == Set("mlsql_example"))
      var sourceType = loadMLSQLTable.map(f => f.sourceType.get).toSet
      assume(sourceType == Set("solr"))

      val saveMLSQLTable = tables
        .filter(f => (f.tableType == TableType.SOLR && f.operateType == OperateType.SAVE))
      db = saveMLSQLTable.map(f => f.db.get).toSet
      assume(db == Set("mlsql_example"))
      sourceType = saveMLSQLTable.map(f => f.sourceType.get).toSet
      assume(sourceType == Set("solr"))
    }

  }

  "auth-hbase" should "work fine" in {


    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession


      val mlsql =
        """
          |connect hbase where
          |    namespace="test_ns"
          |and zk="127.0.0.1:2181" as hbase_instance;
          |
          |load hbase.`hbase_instance:test_tb` options
          |family="cf"
          |as test_table;
          |
          |load hbase.`test_ns_1:test_tb_1`
          |options zk="127.0.0.1:2181"
          |and family="cf"
          |as output1;
          |
          |save overwrite test_table as hbase.`hbase_instance:test_tb_2` where
          |    rowkey="rowkey"
          |and family="cf";
        """.stripMargin
      executeScript(mlsql)

      var tables = DefaultConsoleClient.get

      val loadMLSQLTable = tables.filter(f => (f.tableType == TableType.HBASE && f.operateType == OperateType.LOAD))

      var table = loadMLSQLTable.map(f => f.table.get).toSet
      assume(table == Set("test_tb", "test_tb_1"))
      var db = loadMLSQLTable.map(f => f.db.get).toSet
      assume(db == Set("test_ns", "test_ns_1"))
      var sourceType = loadMLSQLTable.map(f => f.sourceType.get).toSet
      assume(sourceType == Set("hbase"))

      val saveMLSQLTable = tables
        .filter(f => (f.tableType == TableType.HBASE && f.operateType == OperateType.SAVE))
      table = saveMLSQLTable.map(f => f.table.get).toSet
      assume(table == Set("test_tb_2"))
      db = saveMLSQLTable.map(f => f.db.get).toSet
      assume(db == Set("test_ns"))
      sourceType = saveMLSQLTable.map(f => f.sourceType.get).toSet
      assume(sourceType == Set("hbase"))
    }

  }
  "auth-set-statement" should "work fine" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val mlsql =
        """
      set jack=`echo` where type="shell";
      """.stripMargin
      executeScript(mlsql)
      val loadMLSQLTable = DefaultConsoleClient.get.filter(f => (f.tableType == TableType.GRAMMAR && f.operateType == OperateType.SET))
      assert(loadMLSQLTable.size == 1)
    }

  }


  "when table is valirable" should "still work in MLSQL auth" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val mlsql =
        """
          |set load_path = "test/t1";
          |set tmp_table = "tt4";
          |set tmp_table_1 = "tt4_1";
          |set savePath = "download/123.csv";
          |
          |load csv.`${load_path}` options owner = "zhuml" and header="true" and delimiter="," as `${tmp_table}`;
          |
          |select * from `${tmp_table}` limit 50000 as `${tmp_table_1}`;
        """.stripMargin
      executeScript(mlsql)
      val loadMLSQLTable = DefaultConsoleClient.get.filter(f => (f.tableType == TableType.TEMP && f.operateType == OperateType.LOAD))
      assert(loadMLSQLTable.head.table.get == "tt4")

    }
  }

  "auth distinguish select of insert " should "still work in MLSQL auth" in {
    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { implicit runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession

      val mlsql =
        """
          |insert into a
          |select * from b;
        """.stripMargin
      executeScript(mlsql)
      val insert = DefaultConsoleClient.get.filter(f => (f.table.get == "a")).head.operateType
      val select = DefaultConsoleClient.get.filter(f => (f.table.get == "b")).head.operateType
      assert(insert == OperateType.INSERT)
      assert(select == OperateType.SELECT)
    }
  }


}
