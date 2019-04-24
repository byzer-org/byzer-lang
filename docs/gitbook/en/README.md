# MLSQL

MLSQL is a SQL-Based language, and it's also a distributed compute engine based on Spark.
The design goal of the MLSQL is to unify Big Data and Machine Learning, one language, one platform. 

# Show me the examples

Here are some examples show you how to do batch/stream/ml jobs with MLSQL.

## Batch



```sql
-- mock some data
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';

-- load mock data, you can also load data in e.g. jdbc/mogndb/es/hive/hdfs
load jsonStr.`rawData` as table1;

select jack+2 as newjack from table1 -- stansard sql 
as table2; -- name the output of this sql

save overwrite table2 as parquet.`/tmp/table2`; 
```


## Streaming 


```sql
-- the stream name, should be uniq.
set streamName="streamExample";

-- mock some data.
set data='''
{"key":"yes","value":"a,b,c","topic":"test","partition":0,"offset":0,"timestamp":"2008-01-24 18:01:01.001","timestampType":0}
{"key":"yes","value":"d,f,e","topic":"test","partition":0,"offset":1,"timestamp":"2008-01-24 18:01:01.002","timestampType":0}
{"key":"yes","value":"k,d,j","topic":"test","partition":0,"offset":2,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"m,d,z","topic":"test","partition":0,"offset":3,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"o,d,d","topic":"test","partition":0,"offset":4,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
{"key":"yes","value":"m,m,m","topic":"test","partition":0,"offset":5,"timestamp":"2008-01-24 18:01:01.003","timestampType":0}
''';

-- load data as table
load jsonStr.`data` as datasource;

-- convert table as stream source
load mockStream.`datasource` options 
stepSizeRange="0-3"
and valueFormat="csv"
and valueSchema="st(field(column1,string),field(column2,string),field(column3,string))"
as newkafkatable1;

-- aggregation 
select column1,column2,column3,kafkaValue from newkafkatable1 
as table21;

-- output the the result to console.
save append table21  
as console.`` 
options mode="Append"
and duration="10"
and checkpointLocation="/tmp/cpl3";
```



## Machine Learning


```sql
-- load libsvm format data
load libsvm.`sample_libsvm_data.txt` as data;

-- train it with RandomForest and save the model in /tmp/model
train data as RandomForest.`/tmp/model`;

-- register the model as function
register RandomForest.`/tmp/model` as rf_predict;

-- predict it
select predict(features)  from data as result;
```

## Deep Learning

MLSQL have built-in BigDL and supports any python  ML frameworks.

Here is a example Of BigDL. 

```sql
train trainData as BigDLClassifyExt.`/tmp/bigdl` where
disableSparkLog = "true"
and fitParam.0.featureSize="[3,28,28]"
and fitParam.0.classNum="10"
and fitParam.0.maxEpoch="10"
and fitParam.0.code='''
def apply(params:Map[String,String])= {
val model = Sequential()
model.add(Reshape(Array(3, 28, 28), inputShape = Shape(28, 28, 3)))
model.add(Dense(params("classNum").toInt, activation = "softmax").setName("fc2"))
}} '''
```

## Python ML framework supports

```sql
set modelPath="/testStreamingB";

-- 创建依赖环境
select 1 as a as fakeTable;
run fakeTable as PythonEnvExt.`/tmp/jack` where condaYamlFilePath="${HOME}/testStreamingB" 
and command="create";

!createPythonEnv "/tmp/jack" "/testStreamingB"

load csv.`/testStreamingB` as testData;

run testData as PythonAlg.`${modelPath}`
where pythonScriptPath="${HOME}/testStreamingB"    -- the python project
and `fitParam.0.a`="wow"    -- train parameters
and keepVersion="false";    -- is keep version

```


