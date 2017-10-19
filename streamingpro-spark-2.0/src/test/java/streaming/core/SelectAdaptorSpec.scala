package streaming.core

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.streaming.BasicSparkOperation
import streaming.dsl.{ScriptSQLExec, ScriptSQLExecListener}

/**
  * Created by allwefantasy on 19/10/2017.
  */
class SelectAdaptorSpec extends BasicSparkOperation {
  "select with <>" should "work fine" in {

    val input =
      """
        |-- ####################################################################################################
        |-- # Table:	test
        |-- # 中文表名:	商业站点活跃用户表
        |-- # 说明：   	访问过
        |-- # Author:  	xuqs
        |-- # Date:		2017/8/31
        |-- ####################################################################################################
        |
        |select 1 as id -- this is id
        |union all
        |select 2 as id -- this is good
        |as wq_tmp_test1
        |;
      """.stripMargin
    //    val sparkSession = new SparkSession {
    //      var m_sql = ""
    //      var m_table = ""
    //
    //      override def sql(sqlText: String): DataFrame = {
    //        m_sql = sqlText
    //        new DataFrame(this, null, null) {
    //          override def createOrReplaceTempView(viewName: String): Unit = {
    //            m_table = viewName
    //          }
    //        }
    //      }
    //    }
    //    ScriptSQLExec.parse(input, new ScriptSQLExecListener(sparkSession, null))
    //    assert(sparkSession.m_table == "wq_tmp_test1")
    //    assert(sparkSession.m_sql.split("\n").length == 3)

  }
}



