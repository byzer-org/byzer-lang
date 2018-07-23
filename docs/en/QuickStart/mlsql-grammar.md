## MLSQL Grammar

MLSQL support following sentence pattern:

1. connect  (connect to specify external storage engine.)
2. set      (set some property)
3. select   (processing data)
4. train    (processing data or train Algorithm)
5. register (register algorithm model /data processing model)
6. save      (save table to HDFS/eternal storage engine)


### Load data as Table

Data in MLSQL only one format, Table. So if you have other formats eg. CSV, JSon, Parquet, ElasticSearch index,
We should load them as tables first. So you can process the table in SQL or train modular.
 
```sql
load csv.`/tmp/abc.csv` as table1;
```

here is the template:

```sql
load format.`path` as tableName;
```

Sometimes , we need to specify some options to load csv file, you can do like this：

```sql
load csv.`/tmp/abc.csv`
options header="true"
as table1;
```

### Using select to process table




 
 