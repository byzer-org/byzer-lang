## MLSQL

MLSQL通过添加了两个特殊语法，使得SQL也能够支持机器学习。


### 模型训练

语法：

```sql
-- 从tableName获取数据，通过where条件对Algorithm算法进行参数配置并且进行模型训练，最后
-- 训练得到的模型会保存在path路径。
train [tableName] as [Algorithm].[path] where [booleanExpression]
```

比如：

```sql
train data as RandomForest.`/tmp/model` where inputCol="featrues" and maxDepth="3"
```

这句话表示使用对表data中的featrues列使用RandomForest进行训练，树的深度为3。训练完成后的模型会保存在`tmp/model`。

很简单对么？

如果需要知道算法的输入格式以及算法的参数,可以参看[Spark MLlib](https://spark.apache.org/docs/latest/ml-guide.html)。
在MLSQL中，输入格式和算法的参数和Spark MLLib保持一致。

### 样本不均衡问题

为了解决样本数据不平衡问题，所有模型（目前只支持贝叶斯）都支持一种特殊的训练方式。假设我们是一个二分类，A,B。 A 分类有100个样本，B分类有1000个。
差距有十倍。为了得到一个更好的训练效果，我们会训练十个（最大样本数/最小样本数）模型。

第一个模型：

A拿到100,从B随机抽样10%(100/1000),训练。

重复第一个模型十次。

这个可以通过在where条件里把multiModels="true" 即可开启。

在预测函数中，会自动拿到置信度最高模型作为预测结果。


### 预测

语法：

```sql
-- 从Path中加载Algorithm算法对应的模型，并且将该模型注册为一个叫做functionName的函数。
register [Algorithm].[Path] as functionName;
```

比如：

```
register RandomForest.`/tmp/zhuwl_rf_model` as zhuwl_rf_predict;
```

接着我就可以在SQL中使用该函数了：

```
select zhuwl_rf_predict(features) as predict_label, label as original_label from sample_table;
```

很多模型会有多个预测函数。假设我们名字都叫predict

LDA 有如下函数：

* predict  参数为一次int类型，返回一个主题分布。
* predict_doc 参数为一个int数组，返回一个主题分布




### ALS

```sql
train data as ALSInPlace.`/tmp/als` where
-- 第一组参数
`fitParam.0.maxIter`="5"
and `fitParam.0.regParam` = "0.01"
and `fitParam.0.userCol` = "userId"
and `fitParam.0.itemCol` = "movieId"
and `fitParam.0.ratingCol` = "rating"
-- 第二组参数    
and `fitParam.1.maxIter`="1"
and `fitParam.1.regParam` = "0.1"
and `fitParam.1.userCol` = "userId"
and `fitParam.1.itemCol` = "movieId"
and `fitParam.1.ratingCol` = "rating"
-- 计算rmse     
and evaluateTable="test"
and ratingCol="rating"
-- 针对用户做推荐，推荐数量为10  
and `userRec` = "10"
-- 针对内容推荐用户，推荐数量为10
-- and `itemRec` = "10"
and coldStartStrategy="drop"
```

你可以查看模型最后的详情：

```sql
load parquet.`/tmp/als/_model_0/meta/0` as models;
select * from models as result;
```

效果如下：

```
+--------------------+--------+--------------------+------------------+-------+-------------+-------------+--------------------+
|           modelPath|algIndex|                 alg|             score| status|    startTime|      endTime|         trainParams|
+--------------------+--------+--------------------+------------------+-------+-------------+-------------+--------------------+
|/tmp/william/tmp/...|       1|org.apache.spark....|1.8793040074492964|success|1532413509977|1532413516579|Map(ratingCol -> ...|
|/tmp/william/tmp/...|       0|org.apache.spark....|1.8709383720062565|success|1532413516584|1532413520291|Map(ratingCol -> ...|
+--------------------+--------+--------------------+------------------+-------+-------------+-------------+--------------------+
```

你可以获取预测结果

```sql
load parquet.`/tmp/a/data/userRec` as userRec;
select * from userRec as result;
```

算法会自动根据evaluateTable 计算rmse,取rmse最小的作为最好的模型。






### NaiveBayes

示例：

```sql
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;
train data as NaiveBayes.`/tmp/bayes_model`;
register NaiveBayes.`/tmp/bayes_model` as bayes_predict;
select bayes_predict(features)  from data as result;
save overwrite result as json.`/tmp/result`;
```

### RandomForest

```sql
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;
train data as RandomForest.`/tmp/model`;
register RandomForest.`/tmp/model` as predict;
select predict(features)  from data as result;
save overwrite result as json.`/tmp/result`;
```

### GBTRegressor

```sql
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;
train data as GBTRegressor.`/tmp/model`;
register GBTRegressor.`/tmp/model` as predict;
select predict(features)  from data as result;
save overwrite result as json.`/tmp/result`;

```


### LDA 

```sql
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_lda_libsvm_data.txt` as data;
train data as LDA.`/tmp/model` where k="10" and maxIter="10";
register LDA.`/tmp/model` as predict;
select predict_v(features)  from data as result;
save overwrite result as json.`/tmp/result`;
```

### KMeans

```sql
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_kmeans_data.txt` as data;
train data as KMeans.`/tmp/model` where k="10";
register KMeans.`/tmp/model` as predict;
select predict(features)  from data as result;
save overwrite result as json.`/tmp/result`;
```


### FPGrowth

abc.csv:

```
body
1 2 5
1 2 3 5
1 2
```

示例:

```sql
load csv.`/tmp/abc.csv` options header="True" as data;
select split(body," ") as items from data as new_dt;
train new_dt as FPGrowth.`/tmp/model` where minSupport="0.5" and minConfidence="0.6" and itemsCol="items";
register FPGrowth.`/tmp/model` as predict;
select predict(items)  from new_dt as result;
save overwrite result as json.`/tmp/result`;
```




### GBTs

```sql
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;
train data as GBTs.`/tmp/model`;
register GBTs.`/tmp/model` as predict;
select predict(features)  from data as result;
save overwrite result as json.`/tmp/result`;
```

### LSVM

```sql
load libsvm.`/Users/allwefantasy/Softwares/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;
train data as LSVM.`/tmp/model` where maxIter="10" and regParam="0.1";
register LSVM.`/tmp/model` as predict;
select predict(features)  from data as result;
save overwrite result as json.`/tmp/result`;
```


### RowMatrix

当你想计算向量两两相似的时候，RowMatrix提供了一个快速高效的方式。比如LDA计算出了每个文档的向量分布，接着我们需要任意给定一个文章，然后找到
相似的文章， 则可以使用RowMatrix进行计算。无论如何，当文档到百万，计算量始终都是很大的（没有优化前会有万亿次计算），所以实际使用可以将数据
先简单分类。之后在类里面再做相似度计算。

另外RowMatrix需要两个字段，一个是向量，一个是数字，数字必须是从0开始递增，用来和矩阵行做对应，方便唯一标记一篇内容。
如果你的数据不是这样的，比如你的文档的唯一编号是断开的或者是一个字符串，那么你可以使用StringIndex 去生成一个字段作为唯一标记。

```sql
load libsvm.`/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_lda_libsvm_data.txt` as data;
train data as LDA.`/tmp/model` where k="10" and maxIter="10";

register LDA.`/tmp/model` as predict;

select *,zhuhl_lda_predict_doc(features) as features from data
as zhuhl_doclist;

train zhuhl_doclist as RowMatrix.`/tmp/zhuhl_rm_model` where inputCol="features" and labelCol="label";
register RowMatrix.`/tmp/zhuhl_rm_model` as zhuhl_rm_predict;

select zhuhl_rm_predict(label) from data
```

### PageRank

PageRank 可以对有关系对的东西进行建模，输入也很简答，只要item-item pair 就行。

```
load csv.`/spark-2.2.0-bin-hadoop2.7/data/mllib/pagerank_data.txt` as data;

select cast(split(_c0," ")[0] as Long) as edgeSrc,cast(split(_c0," ")[1] as Long) as edgeDst from data
as new_data;

train new_data as PageRank.`/tmp/pr_model` ;
register PageRank.`/tmp/pr_model` as zhl_pr_redict;

select zhl_pr_redict(edgeSrc) from new_data;
```




### LogisticRegressor

```sql
load libsvm.`/tmp/lr.csv` options header="True" as lr;
train lr as LogisticRegressor.`/tmp/linear_regression_model`;
register LogisticRegressor.`/tmp/linear_regression_model` as lr_predict;
select lr_predict(features) from lr as result;
save overwrite result as json.`/tmp/lr_result.csv`;
```

