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

package streaming.dsl.mmlib.algs

import org.apache.spark.ml.param.Param
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.udf.UDFManager
import streaming.dsl.mmlib._
import streaming.dsl.mmlib.algs.param.{BaseParams, WowParams}
import streaming.udf.RuntimeCompileScriptFactory

/**
 * Created by allwefantasy on 27/8/2018.
 */
class ScriptUDF(override val uid: String) extends SQLAlg with WowParams {

  def this() = this(BaseParams.randomUID())

  override def skipPathPrefix: Boolean = true

  override def train(df: DataFrame, path: String, params: Map[String, String]): DataFrame = {
    df.sparkSession.emptyDataFrame
  }

  override def load(sparkSession: SparkSession, path: String, params: Map[String, String]): Any = {
    val res = params.get(code.name).getOrElse(sparkSession.table(path).head().getString(0))

    def thr[T](p: Param[T], value: String) = {
      throw new IllegalArgumentException(
        s"${p.parent} parameter ${p.name} given invalid value $value.")
    }

    params.get(lang.name).map { l =>
      if (!lang.isValid(l)) {
        thr(lang, l)
      }
      set(lang, l)
    }

    params.get(udfType.name).map { l =>
      if (!udfType.isValid(l)) thr(udfType, l)
      set(udfType, l)
    }

    params.get(className.name).map { l =>
      if (!className.isValid(l)) thr(className, l)
      set(className, l)
    }

    params.get(methodName.name).map { l =>
      if (!methodName.isValid(l)) thr(methodName, l)
      set(methodName, l)
    }

    params.get(dataType.name).map { l =>
      if (!dataType.isValid(l)) thr(dataType, l)
      set(dataType, l)
    }
    val scriptCacheKey = ScriptUDFCacheKey(
      res, "", $(className), $(udfType), $(methodName), $(dataType), $(lang)
    )

    $(udfType) match {
      case "udaf" =>
        val udaf = RuntimeCompileScriptFactory.getUDAFCompilerBylang($(lang))
        if (!udaf.isDefined) {
          throw new IllegalArgumentException()
        }
        (e: Seq[Expression]) => udaf.get.udaf(e, scriptCacheKey)
      case _ =>
        val udf = RuntimeCompileScriptFactory.getUDFCompilerBylang($(lang))
        if (!udf.isDefined) {
          throw new IllegalArgumentException()
        }
        (e: Seq[Expression]) => udf.get.udf(e, scriptCacheKey)
    }
  }

  override def predict(sparkSession: SparkSession, _model: Any, name: String, params: Map[String, String]): UserDefinedFunction = {
    val func = _model.asInstanceOf[(Seq[Expression]) => ScalaUDF]
    UDFManager.register(sparkSession, name, func)
    null
  }


  override def explainParams(sparkSession: SparkSession): DataFrame = {
    _explainParams(sparkSession)
  }


  override def doc: Doc = Doc(MarkDownDoc,
    """
      |## Script support
      |
      |Script e.g. Python,Scala nested in MLSQL provides more fine-grained control when doing some ETL tasks, as it allows you
      |easily create SQL function with more powerful language which can do complex logical task.
      |
      |Cause the tedious of java's grammar, we will not support java script.
      |
      |Before use ScriptUDF module, you can use
      |
      |```
      |load modelParams.`ScriptUDF` as output;
      |```
      |
      |to check how to configure this module.
      |
      |### Python UDF Script Example
      |
      |```sql
      |-- using set statement to hold your python script
      |-- Notice that the first parameter of function you defined should be self.
      |set echoFun='''
      |
      |def apply(self,m):
      |    return m
      |
      |''';
      |
      |-- load script as a table, every thing in mlsql should be table which
      |-- can be processed more conveniently.
      |load script.`echoFun` as scriptTable;
      |
      |-- register `apply` as UDF named `echoFun`
      |register ScriptUDF.`scriptTable` as echoFun options
      |-- specify which script you choose
      |and lang="python"
      |-- As we know python is not strongly typed language, so
      |-- we should manually spcify the return type.
      |-- map(string,string) means a map with key is string type,value also is string type.
      |-- array(string) means a array with string type element.
      |-- nested is support e.g. array(array(map(string,array(string))))
      |and dataType="map(string,string)"
      |;
      |
      |-- create a data table.
      |set data='''
      |{"a":1}
      |{"a":1}
      |{"a":1}
      |{"a":1}
      |''';
      |load jsonStr.`data` as dataTable;
      |
      |-- using echoFun in SQL.
      |select echoFun(map('a','b')) as res from dataTable as output;
      |```
      |
      |### Scala UDF Script Example
      |
      |```sql
      |set plusFun='''
      |
      |def apply(a:Double,b:Double)={
      |   a + b
      |}
      |
      |''';
      |
      |-- load script as a table, every thing in mlsql should be table which
      |-- can be process more convenient.
      |load script.`plusFun` as scriptTable;
      |
      |-- register `apply` as UDF named `plusFun`
      |register ScriptUDF.`scriptTable` as plusFun
      |;
      |
      |-- create a data table.
      |set data='''
      |{"a":1}
      |{"a":1}
      |{"a":1}
      |{"a":1}
      |''';
      |load jsonStr.`data` as dataTable;
      |
      |-- using echoFun in SQL.
      |select plusFun(1,2) as res from dataTable as output;
      |```
      |
      |
      |### Python UDAF Example
      |
      |```sql
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
      |```
      |
      |### Scala UDAF Script Example
      |
      |```sql
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
      |```
      |
      |
      |### Some tricks
      |
      |You can simplify the definition of UDF register like following:
      |
      |```sql
      |register ScriptUDF.`` as count_board options lang="python"
      |    and methodName="apply"
      |    and dataType="map(string,integer)"
      |    and code='''
      |def apply(self, s):
      |    from collections import Counter
      |    return dict(Counter(s))
      |    '''
      |;
      |```
      |
      |
      |Multi methods defined onetime is also supported.
      |
      |```sql
      |
      |set plusFun='''
      |
      |def apply(a:Double,b:Double)={
      |   a + b
      |}
      |
      |def hello(a:String)={
      |   s"hello: ${a}"
      |}
      |
      |''';
      |
      |
      |load script.`plusFun` as scriptTable;
      |register ScriptUDF.`scriptTable` as plusFun;
      |register ScriptUDF.`scriptTable` as helloFun options
      |methodName="hello"
      |;
      |
      |
      |-- using echoFun in SQL.
      |select plusFun(1,2) as plus, helloFun("jack") as jack as output;
      |```
      |
      |You can also define this methods in a class:
      |
      |```sql
      |
      |set plusFun='''
      |
      |class ScalaScript {
      |    def apply(a:Double,b:Double)={
      |       a + b
      |    }
      |
      |    def hello(a:String)={
      |       s"hello: ${a}"
      |    }
      |}
      |
      |''';
      |
      |
      |load script.`plusFun` as scriptTable;
      |register ScriptUDF.`scriptTable` as helloFun options
      |methodName="hello"
      |and className="ScalaScript"
      |;
      |
      |
      |-- using echoFun in SQL.
      |select helloFun("jack") as jack as output;
      |```
      |
      |
      |
    """.stripMargin)

  final val code: Param[String] = new Param[String](this, "code",
    s"""Scala or Python code snippet""")


  final val lang: Param[String] = new Param[String](this, "lang",
    s"""Which type of language you want. [scala|python]""")
  setDefault(lang, "scala")

  final val udfType: Param[String] = new Param[String](this, "udfType",
    s"""udf or udaf""", (s: String) => {
      s == "udf" || s == "udaf"
    })
  setDefault(udfType, "udf")

  final val className: Param[String] = new Param[String](this, "className",
    s"""the className of you defined in code snippet.""")
  setDefault(className, "")

  final val methodName: Param[String] = new Param[String](this, "methodName",
    s"""the methodName of you defined in code snippet. If the name is apply, this parameter is optional""")
  setDefault(methodName, "apply")

  final val dataType: Param[String] = new Param[String](this, "dataType",
    s"""when you use python udf, you should define return type.""")
  setDefault(dataType, "")
}



