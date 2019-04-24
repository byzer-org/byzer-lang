# Datasource

MLSQL use load statement to load data from many datasource.  Sometimes, we can mock 
some data in script, like this:

```sql

set rawData=''' 
 {"a":1,"b":2}
 {"a":1,"b":3}
''';

load jsonStr.`rawData` as data;
``` 

Except jsonStr, you can load datasource like ElasticSearch,JDBC,HBase,MongoDB,HDFS files with the same way.