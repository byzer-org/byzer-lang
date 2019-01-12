# Python UDAF

这篇文章会用到前面一篇的所有知识点。下面是UDAF的例子：


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