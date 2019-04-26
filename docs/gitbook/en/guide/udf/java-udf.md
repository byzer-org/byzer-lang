# Java UDF

Example:

```sql
set echoFun='''
import java.util.HashMap;
import java.util.Map;
public class UDF {
  public Map<String, Integer[]> apply(String s) {
    Map<String, Integer[]> m = new HashMap<>();
    Integer[] arr = {1};
    m.put(s, arr);
    return m;
  }
}
''';

load script.`echoFun` as scriptTable;

register ScriptUDF.`scriptTable` as funx
options lang="java"
;

-- create a data table.
set data='''
{"a":"a"}
''';
load jsonStr.`data` as dataTable;

select funx(a) as res from dataTable as output;
```

Notice:

> 1. The source code must be a class
> 2. package name is not allowed

Example2:

```sql
set echoFun='''
import java.util.HashMap;
import java.util.Map;
public class Test {
    public Map<String, String> test(String s) {
      Map m = new HashMap<>();
      m.put(s, s);
      return m;
  }
}
''';

load script.`echoFun` as scriptTable;

register ScriptUDF.`scriptTable` as funx
options lang="java"
and className = "Test"
and methodName = "test"
;

-- create a data table.
set data='''
{"a":"a"}
''';
load jsonStr.`data` as dataTable;

select funx(a) as res from dataTable as output;
```
