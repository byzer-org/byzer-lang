# Scala UDAF

下面是一个典型例子：

```sql
set plusFun='''
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
class SumAggregation extends UserDefinedAggregateFunction with Serializable{
    def inputSchema: StructType = new StructType().add("a", LongType)
    def bufferSchema: StructType =  new StructType().add("total", LongType)
    def dataType: DataType = LongType
    def deterministic: Boolean = true
    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0l)
    }
    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val sum   = buffer.getLong(0)
      val newitem = input.getLong(0)
      buffer.update(0, sum + newitem)
    }
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))
    }
    def evaluate(buffer: Row): Any = {
      buffer.getLong(0)
    }
}
''';


--加载脚本
load script.`plusFun` as scriptTable;
--注册为UDF函数 名称为plusFun
register ScriptUDF.`scriptTable` as plusFun options
className="SumAggregation"
and udfType="udaf"
;

set data='''
{"a":1}
{"a":1}
{"a":1}
{"a":1}
''';
load jsonStr.`data` as dataTable;

-- 使用plusFun
select a,plusFun(a) as res from dataTable group by a as output;
```

为了方便使用，我们也可以定义多个方法，然后分别注册：

```sql
set plusFun='''
class A {

    def apply(a:Double,b:Double)={
       a + b
    }
    
    def hello(a:String)={
       s"hello: ${a}"
    }
}
''';


load script.`plusFun` as scriptTable;
register ScriptUDF.`scriptTable` as plusFun where methodName="apply" and className="A";
register ScriptUDF.`scriptTable` as helloFun options
methodName="hello"  and className="A";


-- using echoFun in SQL.
select plusFun(1,2) as plus, helloFun("jack") as jack as output;
```


