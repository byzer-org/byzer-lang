# Python UDF

Example:

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

As mentioned before, we should specify the return value of python function.

```sql
and dataType="map(string,string)"
```

This means the python function will return dictionary, and key,value are both string. For
now, the return type only supports

* string
* float
* double
* integer
* short
* date
* binary
* map
* array

map and array are complex type.  You can use them to construct more complex type, e.g.

```
array(array(map(string,array(string))))
```

this is a legal return type.


You can also write the code in register statement:

```sql
register ScriptUDF.`` as echoFun options
and lang="python"
and dataType="map(string,string)"
and code=''''
def apply(self,m):
    return m
 '''';
```

If you function name is not apply the you should use methodName to specify the name.

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
