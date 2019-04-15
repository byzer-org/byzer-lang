# Python UDF

下面是一个完整的示例：

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

我们前面说到，对于Python UDF,我们需要显示的指定返回数据类型，在示例中是

```sql
and dataType="map(string,string)"
```

这表示返回值是一个字典，并且key和value都是string. Python UDF并不能返回任意类型，只能是如下类型以及对应的组合:

* string
* float
* double
* integer
* short
* date
* binary
* map
* array

其中map 和array 为复杂类型，里面对应的值只能罗列的这些类型。所以我们也可以做比较复杂的结果返回：

```
array(array(map(string,array(string))))
```

上面的类型描述也是合法的。


上面的做法方便做代码分割，udf申明可以放在单独文件，注册动作可以放在另外的文件，之后都通过include来完成整合。我们也有更简单的模式，比如

```sql
register ScriptUDF.`` as echoFun options
and lang="python"
and dataType="map(string,string)"
and code=''''
def apply(self,m):
    return m
 '''';
```

把代码直接写在了code里，而不是先set 再load，最后引用。 如果函数名称不是apply,那么你需要通过methodName 来进行应用，大致如下：

```sql
register ScriptUDF.`` as echoFun options
and lang="python"
and dataType="map(string,string)"
and methodName="jack"
and code=''''
def jack(self,m):
    return m
 '''';
```

另外，我们可以写多个函数，然后通过methodName指定需要哪个。