## Quick Tutorial

Step 1:


Download the jars from the release page: [Release页面](https://github.com/allwefantasy/streamingpro/releases):

1. streamingpro-mlsql-1.1.2.jar
2. ansj_seg-5.1.6.jar
3. nlp-lang-1.7.8.jar

Step 2:

Visit the downloads page: [Spark](https://spark.apache.org/downloads.html), to download Apache Spark 2.2.0 and then unarvhive it.
 
Step 3:

```shell
cd spark-2.2.0-bin-hadoop2.7/

./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[*] \
--name sql-interactive \
--jars ansj_seg-5.1.6.jar,nlp-lang-1.7.8.jar
streamingpro-mlsql-1.1.2.jar    \
-streaming.name sql-interactive    \
-streaming.job.file.path file:///tmp/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift false \
-streaming.enableHiveSupport true
```

`query.json` is a json file contains "{}".

Step 4: 

Open your chrome browser, type the following url:

```
http://127.0.0.1:9003
```

![](https://github.com/allwefantasy/mlsql-web/raw/master/images/WX20180629-105204@2x.png)

Enjoy.


---------------------------------------------------
Run the first Machine Learning Script in MLSQL.

```sql

-- load data from spark distribution 
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;

-- train a NaiveBayes model and save it in /tmp/bayes_model.
-- Here the alg we use  is based on Spark MLlib 
train data as NaiveBayes.`/tmp/bayes_model`;

-- register your model
register NaiveBayes.`/tmp/bayes_model` as bayes_predict;

-- predict all data 
select bayes_predict(features) as predict_label, label  from data as result;

-- save predicted result in /tmp/result with json format
save overwrite result as json.`/tmp/result`;

-- show predict label in web table.
select * from result as res;
```

Please make sure the path `/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` is correct.

Copy and paste the script to the web page, and click `运行`, then you will see the label and predict_label.

Congratulations, you have completed the first Machine Learning script!

----------------------------------------------------

Run the first ETL Script In MLSQL.


```sql
select "a" as a,"b" as b
as abc;

-- here we just copy all from table abc and then create a new table newabc.

From Oscar:
-- we just copy all from table abc and create a new table newabc here.

select * from abc
as newabc;

-- save the newabc table to mysql.
save overwrite newabc
as jdbc.`db.abc`
options truncate="true"
and driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/...."
and driver="com.mysql.jdbc.Driver"
and user="..."
and password="...."
```

Congratulations, you have completed the first ETL script!

-------------------------------------------------------
