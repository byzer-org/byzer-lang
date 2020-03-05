package streaming.core.message

import org.apache.spark.MLSQLConf

/**
  * 2019-01-13 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLMessage {

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
