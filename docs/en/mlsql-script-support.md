## Script support

Script e.g. Python,Scala nested in MLSQL provides more fine-grained control when doing some ETL tasks, as it allows you 
easily create SQL function with more powerful language which can do complex logical task.
 
Cause the tedious of java's grammar, we will not support java script.


### Python Script Example

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

### Scala Script Example
 
```sql
-- using set statement to hold your python script
-- Notice that the first parameter of function you defined should be self.  
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

