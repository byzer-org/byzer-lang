package streaming.core.message

import org.apache.spark.MLSQLConf

/**
  * 2019-01-13 WilliamZhu(allwefantasy@gmail.com)
  */
object MLSQLMessage {

  val MLSQL_CLUSTER_PS_ENABLE_NOTICE = s"""
             |------------------------------------------------------------------------
             |MLSQL detects that ${MLSQLConf.MLSQL_CLUSTER_PS_ENABLE.key} is enabled.
             |
             |Please make sure you have the uber-jar of mlsql placed in
             |1. --jars
             |2. --conf "spark.executor.extraClassPath=[your jar name in jars]"
             |
             |for exmaple:
             |
             |--jars ./streamingpro-mlsql-spark_2.x-x.x.x-SNAPSHOT.jar
             |--conf "spark.executor.extraClassPath=streamingpro-mlsql-spark_2.x-x.x.x-SNAPSHOT.jar"
             |
             |Notice that when you configure `spark.executor.extraClassPath`,please just assign the jar name
             |without any path prefix.
             |
             |Otherwise the executor will fail to start and the whole application will fails.
             |------------------------------------------------------------------------
          """.stripMargin


  val PYTHON_REQUEIRE_MLSQL_CLUSTER_PS_ENABLE = s"""
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
