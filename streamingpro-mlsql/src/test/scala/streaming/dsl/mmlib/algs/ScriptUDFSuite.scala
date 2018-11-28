package streaming.dsl.mmlib.algs

import scala.collection.mutable.WrappedArray

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}
import streaming.dsl.ScriptSQLExec

/**
 * Created by fchen on 2018/11/15.
 */
class ScriptUDFSuite extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {

  "test scala script map return type" should "work fine" in {
    withBatchContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
          |  set echoFun='''
          |
          |  def apply(m: String) = {
          |     Map("a" -> Array[Int](1))
          |  }
          |  ''';
          |
          |  load script.`echoFun` as scriptTable;
          |
          |  register ScriptUDF.`scriptTable` as funx
          |  ;
          |
          |  -- create a data table.
          |  set data='''
          |  {"a":"a"}
          |  ''';
          |  load jsonStr.`data` as dataTable;
          |
          |  select funx(a) as res from dataTable as output;
          |
        """.stripMargin, sq)

      val result = runtime.sparkSession.sql("select * from output").collect()
      assert(result.size == 1)
      assert(result.head.getAs[Map[String, WrappedArray[Int]]](0)("a").head == 1)
    }
  }


  "test scala compile error case" should "work fine" in {
    withBatchContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      val query: (String) => String = (scalaCode) => s"""
          |  set echoFun='''
          |  ${scalaCode}
          |  ''';
          |
          |  load script.`echoFun` as scriptTable;
          |
          |  register ScriptUDF.`scriptTable` as funx
          |  ;
          |
          |  -- create a data table.
          |  set data='''
          |  {"a":"a"}
          |  ''';
          |  load jsonStr.`data` as dataTable;
          |
          |  select funx(a) as res from dataTable as output;
          |
        """.stripMargin

      val scalaCode =
        """
          |  def apply(m: String) = {
          |     Map("a" -> Array[Int](1))
          |  }
          |  apply("hello")
        """.stripMargin


      assertThrows[IllegalArgumentException] {
        ScriptSQLExec.parse(query(scalaCode), sq)
      }

      val functionWithOtherName =
        """
          |  def function(m: String) = {
          |     Map("a" -> Array[Int](1))
          |  }
        """.stripMargin
      sq = createSSEL

      assertThrows[IllegalArgumentException] {
        ScriptSQLExec.parse(query(functionWithOtherName), sq)
      }



    }
  }

  "test python script map return type" should "work fine" in {
    withBatchContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
          |set dictFun='''
          |
          |def apply(self,m):
          |    dict = {m: m}
          |    return dict
          |''';
          |
          |load script.`dictFun` as scriptTable;
          |register ScriptUDF.`scriptTable` as dictFun options
          |and lang="python"
          |and dataType="map(string,string)"
          |;
          |set data='''
          |{"a":"1"}
          |{"a":"2"}
          |{"a":"3"}
          |{"a":"4"}
          |''';
          |load jsonStr.`data` as dataTable;
          |select dictFun(a) as res from dataTable as output;
        """.stripMargin, sq)

      val result = runtime.sparkSession.sql("select * from output").collect()
      assert(result.size == 4)
      val sample = result.head.getAs[Map[String, String]](0).head
      assert(sample._1 == sample._2)
    }
  }

  "test python script import" should "work fine" in {
    withBatchContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
          |set jsonFun='''
          |
          |def apply(self,m):
          |    import json
          |    d = json.loads(m)
          |    return d['key']
          |''';
          |
          |load script.`jsonFun` as scriptTable;
          |register ScriptUDF.`scriptTable` as jsonFun options
          |and lang="python"
          |and dataType="string"
          |;
          |select jsonFun("{\"key\": \"value\"}") as res as output;
        """.stripMargin, sq)

      val result = runtime.sparkSession.sql("select * from output").collect()
      assert(result.size == 1)
      val sample = result.head.getString(0)
      assert(sample == "value")
    }
  }

  "test scala udaf" should "work fine" in {
    withBatchContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
           |set plusFun='''
           |import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
           |import org.apache.spark.sql.types._
           |import org.apache.spark.sql.Row
           |class SumAggregation extends UserDefinedAggregateFunction with Serializable{
           |    def inputSchema: StructType = new StructType().add("a", LongType)
           |    def bufferSchema: StructType =  new StructType().add("total", LongType)
           |    def dataType: DataType = LongType
           |    def deterministic: Boolean = true
           |    def initialize(buffer: MutableAggregationBuffer): Unit = {
           |      buffer.update(0, 0l)
           |    }
           |    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
           |      val sum   = buffer.getLong(0)
           |      val newitem = input.getLong(0)
           |      buffer.update(0, sum + newitem)
           |    }
           |    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
           |      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
           |    }
           |    def evaluate(buffer: Row): Any = {
           |      buffer.getLong(0)
           |    }
           |}
           |''';
           |
           |load script.`plusFun` as scriptTable;
           |register ScriptUDF.`scriptTable` as plusFun options
           |
           |className="SumAggregation"
           |and udfType="udaf"
           |;
           |
           |set data='''
           |{"a":1}
           |{"a":1}
           |{"a":1}
           |{"a":1}
           |''';
           |load jsonStr.`data` as dataTable;
           |select a,plusFun(a) as res from dataTable group by a as output;
           |
        """.stripMargin, sq)

      val result = runtime.sparkSession.sql("select * from output").collect()
      assert(result.size == 1)
      assert(result.head.getAs[Long]("res") == 4)
    }
  }

  "test python udaf" should "work fine" in {
    withBatchContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      implicit val spark = runtime.sparkSession
      var sq = createSSEL
      ScriptSQLExec.parse(
        """
          |set plusFun='''
          |from org.apache.spark.sql.expressions import MutableAggregationBuffer, UserDefinedAggregateFunction
          |from org.apache.spark.sql.types import DataTypes,StructType
          |from org.apache.spark.sql import Row
          |import java.lang.Long as l
          |import java.lang.Integer as i
          |
          |class SumAggregation:
          |
          |    def inputSchema(self):
          |        return StructType().add("a", DataTypes.LongType)
          |
          |    def bufferSchema(self):
          |        return StructType().add("total", DataTypes.LongType)
          |
          |    def dataType(self):
          |        return DataTypes.LongType
          |
          |    def deterministic(self):
          |        return True
          |
          |    def initialize(self,buffer):
          |        return buffer.update(i(0), l(0))
          |
          |    def update(self,buffer, input):
          |        sum = buffer.getLong(i(0))
          |        newitem = input.getLong(i(0))
          |        buffer.update(i(0), l(sum + newitem))
          |
          |    def merge(self,buffer1, buffer2):
          |        buffer1.update(i(0), l(buffer1.getLong(i(0)) + buffer2.getLong(i(0))))
          |
          |    def evaluate(self,buffer):
          |        return buffer.getLong(i(0))
          |''';
          |
          |
          |load script.`plusFun` as scriptTable;
          |register ScriptUDF.`scriptTable` as plusFun options
          |className="SumAggregation"
          |and udfType="udaf"
          |and lang="python"
          |;
          |
          |set data='''
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |''';
          |load jsonStr.`data` as dataTable;
          |
          |select a,plusFun(a) as res from dataTable group by a as output;
        """.stripMargin, sq)

      val result = runtime.sparkSession.sql("select * from output").collect()
      assert(result.size == 1)
      assert(result.head.getAs[Long]("res") == 4)
    }
  }

  "test ScalaRuntimeCompileUDAF" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams)) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL

      ScriptSQLExec.parse(
        """
          |set plusFun='''
          |import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
          |import org.apache.spark.sql.types._
          |import org.apache.spark.sql.Row
          |class SumAggregation extends UserDefinedAggregateFunction with Serializable{
          |    def inputSchema: StructType = new StructType().add("a", LongType)
          |    def bufferSchema: StructType =  new StructType().add("total", LongType)
          |    def dataType: DataType = LongType
          |    def deterministic: Boolean = true
          |    def initialize(buffer: MutableAggregationBuffer): Unit = {
          |      buffer.update(0, 0l)
          |    }
          |    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
          |      val sum   = buffer.getLong(0)
          |      val newitem = input.getLong(0)
          |      buffer.update(0, sum + newitem)
          |    }
          |    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
          |      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
          |    }
          |    def evaluate(buffer: Row): Any = {
          |      buffer.getLong(0)
          |    }
          |}
          |''';
          |
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScriptUDF.`scriptTable` as plusFun options
          |className="SumAggregation"
          |and udfType="udaf"
          |;
          |
          |set data='''
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |''';
          |load jsonStr.`data` as dataTable;
          |
          |-- 使用plusFun
          |select a,plusFun(a) as res from dataTable group by a as output;
        """.stripMargin, sq)
      val res = spark.sql("select * from output").collect().head.get(1)
      assert(res == 4)
    }
  }

  "test PythonRuntimeCompileUDAF" should "work fine" in {

    withBatchContext(setupBatchContext(batchParams)) { runtime: SparkRuntime =>
      //执行sql
      implicit val spark = runtime.sparkSession
      val sq = createSSEL

      ScriptSQLExec.parse(
        """
          |set plusFun='''
          |from org.apache.spark.sql.expressions import MutableAggregationBuffer, UserDefinedAggregateFunction
          |from org.apache.spark.sql.types import DataTypes,StructType
          |from org.apache.spark.sql import Row
          |import java.lang.Long as l
          |import java.lang.Integer as i
          |
          |class SumAggregation:
          |
          |    def inputSchema(self):
          |        return StructType().add("a", DataTypes.LongType)
          |
          |    def bufferSchema(self):
          |        return StructType().add("total", DataTypes.LongType)
          |
          |    def dataType(self):
          |        return DataTypes.LongType
          |
          |    def deterministic(self):
          |        return True
          |
          |    def initialize(self,buffer):
          |        return buffer.update(i(0), l(0))
          |
          |    def update(self,buffer, input):
          |        sum = buffer.getLong(i(0))
          |        newitem = input.getLong(i(0))
          |        buffer.update(i(0), l(sum + newitem))
          |
          |    def merge(self,buffer1, buffer2):
          |        buffer1.update(i(0), l(buffer1.getLong(i(0)) + buffer2.getLong(i(0))))
          |
          |    def evaluate(self,buffer):
          |        return buffer.getLong(i(0))
          |''';
          |
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScriptUDF.`scriptTable` as plusFun options
          |className="SumAggregation"
          |and udfType="udaf"
          |and lang="python"
          |;
          |
          |set data='''
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |''';
          |load jsonStr.`data` as dataTable;
          |
          |-- 使用plusFun
          |select a,plusFun(a) as res from dataTable group by a as output;
        """.stripMargin, sq)
      val res = spark.sql("select * from output").collect().head.get(1)
      assume(res == 4)
    }
  }


}
