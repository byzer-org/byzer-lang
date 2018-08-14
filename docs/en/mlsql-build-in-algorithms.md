## MLSQL

- Feature engineer modules
    - Text
        - [TfIdfInPlace](#tfidfinplace)
        - [Word2VecInPlace](#word2vecinplace)
        - [ScalerInPlace](#scalerinplace)
        - [ConfusionMatrix](#confusionmatrix)
        - [NormalizeInPlace](#normalizeinplace)
        - [ModelExplainInPlace](#modelexplaininplace)
        - [Discretizer](#discretizer)
        - [bucketizer](#bucketizer方式)
        - [quantile](#quantile方式)        
        - [VecMapInPlace](#vecmapinplace)        
        - [TokenExtract / TokenAnalysis](#tokenextract--tokenanalysis)
        - [RateSampler](#ratesampler)
        - [RowMatrix](#rowmatrix)
        - [CommunityBasedSimilarityInPlace](#communitybasedsimilarityonplace)
        - [Word2ArrayInPlace](#word2arrayinplace)
    - Image
        - [OpenCVImage](#opencvimage)
        - [JavaImage](#javaimage)
- Collaborative filtering
    - [ALS](#als)
- Classification    
    - [NaiveBayes](#naivebayes)
    - [RandomForest](#randomforest)
    - [GBTRegressor](#gbtregressor)
- Clustering
    - [LDA](#lda)
    - [KMeans](#kmeans)
- [FPGrowth](#fpgrowth)
- [GBTs](#gbts)
- [PageRank](#pagerank)
- [LogisticRegressor](#logisticregressor)


### TfIdfInPlace

TfIdfInPlace is used to convert raw text to vector.

The processing steps include:

1. word analysis
2. filter with stopwords 
3. ngram
4. word indexed with integer
5. compute idf/tf
6. weighted specified words


Example:

```sql
train orginal_text_corpus as TfIdfInPlace.`/tmp/tfidfinplace`
where inputCol="content"

-- analysis options 
and ignoreNature="true"
and dicPaths="...."
and stopWordPath="/tmp/tfidf/stopwords"

-- words should be weighted
and priorityDicPath="/tmp/tfidf/prioritywords"
and priority="5.0"

-- ngram 
and nGram="2,3"
;

load parquet.`/tmp/tfidf/data` as lwys_corpus_with_featurize; 

register TfIdfInPlace.`/tmp/tfidfinplace` as predict;
```


### Word2VecInPlace

Word2VecInPlace is used to convert raw text to vector/sequence.

The processing steps include:

1. word analysis
2. filter with stopwords 
3. ngram
4. word indexed with integer
5. convert integer to vector
6. returns 1/2d array according to the requirements 

If you declare word embedding directory  explicitly, Word2VecInPlace will skip the step training with word2vec algorithm.

Example：

```sql
load parquet.`/tmp/tfidf/df`
as orginal_text_corpus;

train orginal_text_corpus as Word2VecInPlace.`/tmp/word2vecinplace`
where inputCol="content"
and ignoreNature="true"
and stopWordPath="/tmp/tfidf/stopwords"
and resultFeature="flat";

load parquet.`/tmp/word2vecinplace/data` 
as lwys_corpus_with_featurize;

register Word2VecInPlace.`/tmp/tfidfinplace` as predict;
```

Parameters:

|Parameter|Default|Comments|
|:----|:----|:----|
|inputCol|None||
|resultFeature|None|flag:flat n-dim array to 1-dim array;merge: merge n-dim array；index: output word sequence|
|dicPaths|None||
|wordvecPaths|None||
|vectorSize|None||
|length|None|input sentence length|
|stopWordPath|None||
|split|None||
|minCount|None||


### ALS

ALS is the abbreviation of "alternating least squares", and it is a implementation of Collaborative filtering which aims 
to fill in the missing entries of a user-item association matrix.

Example:

```sql
train data as ALSInPlace.`/tmp/als` where

-- the first group of parameters
`fitParam.0.maxIter`="5"
and `fitParam.0.regParam` = "0.01"
and `fitParam.0.userCol` = "userId"
and `fitParam.0.itemCol` = "movieId"
and `fitParam.0.ratingCol` = "rating"

-- the sencond group of parameters    
and `fitParam.1.maxIter`="1"
and `fitParam.1.regParam` = "0.1"
and `fitParam.1.userCol` = "userId"
and `fitParam.1.itemCol` = "movieId"
and `fitParam.1.ratingCol` = "rating"

-- compute rmse     
and evaluateTable="test"
and ratingCol="rating"

-- size of recommending items for user  
and `userRec` = "10"

-- size of recommending users for item
-- and `itemRec` = "10"
and coldStartStrategy="drop"
```


Load model-meta-path:

```sql
load parquet.`/tmp/als/_model_0/meta/0` as models;
select * from models as output;
```

Here the metas: 

```
+--------------------+--------+--------------------+------------------+-------+-------------+-------------+--------------------+
|           modelPath|algIndex|                 alg|             score| status|    startTime|      endTime|         trainParams|
+--------------------+--------+--------------------+------------------+-------+-------------+-------------+--------------------+
|/tmp/william/tmp/...|       1|org.apache.spark....|1.8793040074492964|success|1532413509977|1532413516579|Map(ratingCol -> ...|
|/tmp/william/tmp/...|       0|org.apache.spark....|1.8709383720062565|success|1532413516584|1532413520291|Map(ratingCol -> ...|
+--------------------+--------+--------------------+------------------+-------+-------------+-------------+--------------------+
```

Load  model-data-path:

```sql
load parquet.`/tmp/a/data/userRec` as userRec;
select * from userRec as result;
```

Note: the algorithm find the best model with the smallest value of RMSE. 



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

Example:

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



### PageRank

PageRank take item-item pair as input, and calculate the weight of edge.

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

