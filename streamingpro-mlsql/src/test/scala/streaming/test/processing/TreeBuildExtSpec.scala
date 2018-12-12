package streaming.test.processing

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec

/**
  * 2018-12-12 WilliamZhu(allwefantasy@gmail.com)
  */
class TreeBuildExtSpec extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {
  "treeBuildExt" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
          |set jsonStr = '''
          |{"id":0,"parentId":null}
          |{"id":1,"parentId":null}
          |{"id":2,"parentId":1}
          |{"id":3,"parentId":1}
          |{"id":7,"parentId":0}
          |{"id":199,"parentId":1}
          |''';
          |
          |load jsonStr.`jsonStr` as data;
          |run data as TreeBuildExt.`` where idCol="id" and parentIdCol="parentId" as result;
        """.stripMargin, sq)
      spark.sql("select * from result").show(false)
      assume(spark.sql("select * from result").count() == 2)
    }
  }


  "treeBuildExt id=parentid" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams, "classpath:///test/empty.json")) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
          |set jsonStr = '''
          |{"id":0,"parentId":null}
          |{"id":1,"parentId":null}
          |{"id":2,"parentId":1}
          |{"id":3,"parentId":3}
          |{"id":7,"parentId":0}
          |{"id":199,"parentId":1}
          |{"id":200,"parentId":199}
          |''';
          |
          |load jsonStr.`jsonStr` as data;
          |run data as TreeBuildExt.`` where idCol="id" and parentIdCol="parentId" as result;
        """.stripMargin, sq)
      spark.sql("select * from result").show(false)
      assume(spark.sql("select * from result").count() == 2)
    }
  }


}
