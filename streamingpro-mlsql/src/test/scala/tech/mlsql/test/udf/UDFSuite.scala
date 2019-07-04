package tech.mlsql.test.udf

import org.apache.spark.streaming.BasicSparkOperation
import streaming.core.strategy.platform.SparkRuntime
import streaming.core.{BasicMLSQLConfig, SpecFunctions}

/**
  * 2019-07-04 WilliamZhu(allwefantasy@gmail.com)
  */
class UDFSuite extends BasicSparkOperation with SpecFunctions with BasicMLSQLConfig {


  "udf" should "scala udf" in {
    withContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      val items = executeCode(runtime,
        """
          |set plusFun='''
          |class PlusFun{
          |  def plusFun(a:Double,b:Double)={
          |   a + b
          |  }
          |}
          |''';
          |
          |load script.`plusFun` as scriptTable;
          |register ScalaScriptUDF.`scriptTable` as plusFun options
          |className="PlusFun"
          |and methodName="plusFun"
          |;
          |
          |select plusFun(1,1) as res as output;
        """.stripMargin)
      assert(items.head.getDouble(0) == 2)


    }
  }

  "udf" should "python udf" in {
    withContext(setupBatchContext(batchParamsWithoutHive)) { runtime: SparkRuntime =>
      var items = executeCode(runtime,
        """
          |
          |-- 填写script脚本
          |set plusFun='''
          |def plusFun(self,a,b,c,d,e,f):
          |    return a+b+c+d+e+f
          |''';
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScriptUDF.`scriptTable` as plusFun options
          |and methodName="plusFun"
          |and lang="python"
          |and dataType="integer"
          |;
          |set data='''
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |''';
          |load jsonStr.`data` as dataTable;
          |-- 使用plusFun
          |select plusFun(1, 2, 3, 4, 5, 6) as res from dataTable as output;
        """.stripMargin)
      assert(items.head.getInt(0) == 21)
      items = executeCode(runtime,
        """
          |
          |set plusFun='''
          |def plusFun(self,m):
          |    return m
          |''';
          |
          |--加载脚本
          |load script.`plusFun` as scriptTable;
          |--注册为UDF函数 名称为plusFun
          |register ScriptUDF.`scriptTable` as plusFun options
          |and methodName="plusFun"
          |and lang="python"
          |and dataType="map(string,string)"
          |;
          |set data='''
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |{"a":1}
          |''';
          |load jsonStr.`data` as dataTable;
          |-- 使用plusFun
          |select plusFun(map('a','b')) as res from dataTable as output;
        """.stripMargin)
      val value = items.head.getMap(0)("a").asInstanceOf[String]
      assert(value == "b")


    }
  }


}
