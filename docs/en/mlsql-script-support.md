## Script support

Script e.g. Python,Scala nested in MLSQL provides more fine-grained control when doing some ETL tasks, as it allows you 
easily create SQL function with more powerful language which can do complex logical task.
 
Cause the tedious of java's grammar, we will not support java script.


### Python UDF Script Example

```sql
-- using set statement to hold your python script
-- Notice that the first parameter of function you defined should be self.  
set echoFun='''

def apply(self,m):
    return m
    
''';

-- load script as a table, every thing in mlsql should be table which 
-- can be processed more conveniently.
load script.`echoFun` as scriptTable;

-- register `apply` as UDF named `echoFun` 
register ScriptUDF.`scriptTable` as echoFun options
-- specify which script you choose
and lang="python"
-- As we know python is not strongly typed language, so 
-- we should manually spcify the return type.
-- map(string,string) means a map with key is string type,value also is string type.
-- array(string) means a array with string type element.
-- nested is support e.g. array(array(map(string,array(string))))
and dataType="map(string,string)"
;

-- create a data table.
set data='''
{"a":1}
{"a":1}
{"a":1}
{"a":1}
''';
load jsonStr.`data` as dataTable;

-- using echoFun in SQL.
select echoFun(map('a','b')) as res from dataTable as output;
```

### Scala UDF Script Example
 
```sql
set plusFun='''

def apply(a:Double,b:Double)={
   a + b
}
    
''';

-- load script as a table, every thing in mlsql should be table which 
-- can be process more convenient.
load script.`plusFun` as scriptTable;

-- register `apply` as UDF named `plusFun` 
register ScriptUDF.`scriptTable` as plusFun
;

-- create a data table.
set data='''
{"a":1}
{"a":1}
{"a":1}
{"a":1}
''';
load jsonStr.`data` as dataTable;

-- using echoFun in SQL.
select plusFun(1,2) as res from dataTable as output;
``` 


### Python UDAF Example

```sql
set plusFun='''
from org.apache.spark.sql.expressions import MutableAggregationBuffer, UserDefinedAggregateFunction
from org.apache.spark.sql.types import DataTypes,StructType
from org.apache.spark.sql import Row
import java.lang.Long as l
import java.lang.Integer as i

class SumAggregation:

    def inputSchema(self):
        return StructType().add("a", DataTypes.LongType)

    def bufferSchema(self):
        return StructType().add("total", DataTypes.LongType)

    def dataType(self):
        return DataTypes.LongType

    def deterministic(self):
        return True

    def initialize(self,buffer):
        return buffer.update(i(0), l(0))

    def update(self,buffer, input):
        sum = buffer.getLong(i(0))
        newitem = input.getLong(i(0))
        buffer.update(i(0), l(sum + newitem))

    def merge(self,buffer1, buffer2):
        buffer1.update(i(0), l(buffer1.getLong(i(0)) + buffer2.getLong(i(0))))

    def evaluate(self,buffer):
        return buffer.getLong(i(0))
''';


--加载脚本
load script.`plusFun` as scriptTable;
--注册为UDF函数 名称为plusFun
register ScriptUDF.`scriptTable` as plusFun options
className="SumAggregation"
and udfType="udaf"
and lang="python"
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

### Scala UDAF Script Example

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


### Some tricks

Multi methods defined onetime is also supported.

```sql
 
set plusFun='''

def apply(a:Double,b:Double)={
   a + b
}

def hello(a:String)={
   s"hello: ${a}"
}
    
''';


load script.`plusFun` as scriptTable;
register ScriptUDF.`scriptTable` as plusFun;
register ScriptUDF.`scriptTable` as helloFun options
methodName="hello"
;


-- using echoFun in SQL.
select plusFun(1,2) as plus, helloFun("jack") as jack as output;
```

You can also define this methods in a class:

```sql
 
set plusFun='''

class ScalaScript {
    def apply(a:Double,b:Double)={
       a + b
    }
    
    def hello(a:String)={
       s"hello: ${a}"
    }
}
    
''';


load script.`plusFun` as scriptTable;
register ScriptUDF.`scriptTable` as helloFun options
methodName="hello"
and className="ScalaScript"
;


-- using echoFun in SQL.
select helloFun("jack") as jack as output;
```

