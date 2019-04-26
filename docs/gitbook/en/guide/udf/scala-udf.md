# Scala UDF

Example：

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

Write the code in register statement：


```sql
register ScriptUDF.`` as plusFun where
and lang="scala"
and udfType="udf"
code='''
def apply(a:Double,b:Double)={
   a + b
}
'''; 

```


