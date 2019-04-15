/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  "treeBuildExt with nodeTreePerRow" should "work fine" in {

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
          |{"id":201,"parentId":199}
          |''';
          |
          |load jsonStr.`jsonStr` as data;
          |run data as TreeBuildExt.`` where idCol="id" and parentIdCol="parentId" and treeType="nodeTreePerRow" as result;
        """.stripMargin, sq)
      spark.sql("select * from result").show(false)
      val rows = spark.sql("select * from result").collect()
      val wow200 = rows.filter(f => f.getAs[String]("id") == "200").head
      assume(wow200.getAs[Int]("level") == 0)
      assume(wow200.getSeq[String](2).size == 0)

      val wow1 = rows.filter(f => f.getAs[String]("id") == "1").head
      assume(wow1.getAs[Int]("level") == 2)
      assume(wow1.getSeq[String](2).size == 4)
    }
  }


}
