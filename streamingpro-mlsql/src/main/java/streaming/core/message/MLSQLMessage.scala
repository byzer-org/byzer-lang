package streaming.core.message

import org.apache.spark.MLSQLConf

/**
  * 2019-01-13 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLMessage {

  val MLSQL_CLUSTER_PS_ENABLE_NOTICE =
    s"""
       |------------------------------------------------------------------------
       |MLSQL detects that ${MLSQLConf.MLSQL_CLUSTER_PS_ENABLE.key} is enabled.
       |
             |Please make sure
       |1. you have placed mlsql-ps-service_x.x.x_2.11-x.x.x.jar  in SPARK_HOME/jars
       |
             |You can download the jar from http://download.mlsql.tech/1.4.0-SNAPSHOT/mlsql-ps-services/.
       |Please choose the right version.
       |
             |Otherwise the executor will fail to start and the whole application will fails.
       |------------------------------------------------------------------------
          """.stripMargin


  val PYTHON_REQUEIRE_MLSQL_CLUSTER_PS_ENABLE =
    s"""
       |[Warning]
       |
             |MLSQL detects that you are not in local mode,
       |but ${MLSQLConf.MLSQL_CLUSTER_PS_ENABLE.key} is not set true.
       |
             |PythonAlg and PythonParallelExt will fail if you are in cluster mode and
       |without enabling ${MLSQLConf.MLSQL_CLUSTER_PS_ENABLE.key}
       |
              """.stripMargin

}
