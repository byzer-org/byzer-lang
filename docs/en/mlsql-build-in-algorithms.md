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
        - [Bucketizer](#bucketizer方式)
        - [Quantile](#quantile方式)        
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

- Tools
  - [SendMessage](#sendmessage)
  - [JDBC](#jdbc)

### TfIdfInPlace

TF/IDF is really widespread used in NLP classification job. 
TfIdfInPlace can be used to convert raw text to vector with tf/idf.

A list of numeric is represented by Vector in StreamingPro  which is can be feed to many algorithms directly, and the element in vector 
normally  is double type.

The processing steps include:

1. text analysis
2. filter with stopwords 
3. ngram
4. word indexed with integer
5. compute idf/tf
6. weighted specified words

You can control how this module work by tuning the parameters TfIdfInPlace exposes. 

Example:

```sql
train orginal_text_corpus as TfIdfInPlace.`/tmp/tfidfinplace`
where inputCol="content"
-- optinal, a token specifying how to analysis the text string.
-- and split=" "
-- when analysis text content, we will not 
-- consider POS if set true
and ignoreNature="true"
-- user-defined dictionary
and dicPaths="...."
-- user-defined stopword dictionary
and stopWordPath="/tmp/tfidf/stopwords"
-- words should be weighted
and priorityDicPath="/tmp/tfidf/prioritywords"
-- how much weight should be applied in priority words
and priority="5.0"
-- ngram，we can compose 2 or 3 words together so maby the new 
-- complex features can more succinctly capture importtant information in 
-- raw data. Note that too much ngram composition may increase feature space 
-- too much , this makes it hard to compute.
and nGram="2,3"
;

-- load the converting result.
load parquet.`/tmp/tfidf/data` as lwys_corpus_with_featurize;
-- register the model as predict function so we can use it in batch/stream/api applications 
register TfIdfInPlace.`/tmp/tfidfinplace` as predict;
```


Parameters:

|Parameter|Default|Comments|
|:----|:----|:----|
|inputCol|Which text column you want to process||
|resultFeature|None|flag:concat m n-dim arrays to one m*n-dim array;merge: merge multi n-dim arrays into one n-dim array；index: output of conword sequence|
|dicPaths|None|user-defined dictionary|
|stopWordPath|user-defined stop word dictionary||
|priority||how much weight should be applied in priority words|
|nGram|None|ngram，we can compose 2 or 3 words together so maby the new complex features can more succinctly capture importtant information in raw data. Note that too much ngram composition may increase feature space too much , this makes it hard to compute.|
|split|optinal, a token specifying how to analysis the text string||


### Word2VecInPlace

Word2VecInPlace is similar with TfIdfInPlace. The difference is Word2VecInPlace works on word embedding tech. 

The processing steps include:

1. text analysis
2. filter with stopwords 
3. ngram
4. word indexed with integer
5. convert integer to vector
6. returns 1/2d array according to the requirements 

If you declare word-embedding directory explicitly, Word2VecInPlace will skip the step training with word2vec algorithm.

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
|resultFeature|None|flag:concat m n-dim arrays to one m*n-dim array;merge: merge multi n-dim arrays into one n-dim array；index: output of conword sequence|
|dicPaths|None|user-defined dictionary|
|wordvecPaths|None|you can specify the location of existed word2vec model|
|vectorSize|None|the  word vector size you expect|
|length|None|input sentence length|
|stopWordPath|user-defined stop word dictionary||
|split|optinal, a token specifying how to analysis the text string||
|minCount|None||


## NormalizeInPlace

Models that are smooth functions of input features are 
sensitive to the scale of the input. Other examples include K-means clustering, nearest neighbors methods,
RBF kernels, and anything that uses the Euclidean distance.
For these models and model modeling components, It's a good idea to use NormalizeInPlace/ScalerInPlace module in MLSQL to normalize the features
so that the output can stay on a expected scale.

NormalizeInPlace can be used to normalize double columns. 
Here is the usage:

```sql

train orginal_text_corpus as NormalizeInPlace.`/tmp/scaler2`
where inputCols="a,b"
and method="standard"
and removeOutlierValue="false"
;
register NormalizeInPlace.`/tmp/scaler2` as jack;
```

Parameters:

|parameter|default|comments|
|:----|:----|:----|
|inputCols|None|double type，serparate by comma，or single array&lt;double&gt; column|
|method|standard|standard,p-norm|
|removeOutlierValue|false|replace outlier value with median|

Register model:

```sql
register NormalizeInPlace.`/tmp/scaler2` as jack;
```

Predict data:

```sql
select jack(array(a,b))[0] a,jack(array(a,b))[1] b, c from orginal_text_corpus
```

### ScalerInPlace

ScalerInPlace use some functions e.g. min-max,log2,logn to smooth the value. However it's not 
like NormalizeInPlace, ScalerInPlace works on individual column. 

```sql

train orginal_text_corpus as ScalerInPlace.`/tmp/scaler`
where inputCols="a,b"
and scaleMethod="min-max"
and removeOutlierValue="false"
;

load parquet.`/tmp/scaler/data` 
as featurize_table;
```

Parameters：

|parameter|default|comments|
|:----|:----|:----|
|inputCols|None|double, using comma to separate|
|scaleMethod|log2|supports：minx-max,log2,logn,log10,sqrt,abs|
|removeOutlierValue|false|replace outlier value with median|


### ConfusionMatrix

Once you have finished the training of classification job, the first sanity check for the accuracy is  to compute a confusion
matrix , so you have a overview of the performance of the classification job, and know which category is not good. 

Suppose that you have file  `/Users/dxy_why/confusionMatrixTestData.csv` contains:

```csv
 actual,predict
 cat,dog
 cat,cat
 dog,dog
 cat,rabbit
 rabbit,rabbit
 cat,cat
 dog,dog
 dog,rabbit
 rabbit,rabbit
```

We can compute the confusion matrix like this:

```sql
load csv.`/Users/dxy_why/confusionMatrixTestData.csv` options header="true" 
as table1;

train table1 as ConfusionMatrix.`/Users/dxy_why/tmp/confusionMatrix/` 
-- actual label column ，string
where actualCol="actual" 
-- predicted label column，string
and predictCol="predict";
```

The model is saved in `/Users/dxy_why/tmp/confusionMatrix/data`
```
select  * from parquet.`/Users/dxy_why/tmp/confusionMatrix/data` 
```

Statistics is saved in `/Users/dxy_why/tmp/confusionMatrix/detail`
```
select  * from parquet.`/Users/dxy_why/tmp/confusionMatrix/detail` 
```


Parameters:

|parameter|default|comments|
|:----|:----|:----|
|actualCol|""||
|predictCol|""||


### FeatureExtract

Extracts information eg. phone number, QQ number from text. 

Suppose that you have a file `/Users/dxy_why/featureExtractTestData.csv` contains:

```csv
 请联系 13634282910
 扣扣 527153688@qq.com
 <html> dddd img.dxycdn.com ffff 527153688@qq.com 
```

Using FeatureExtract to process the file:

```
select _c0 as doc
from csv.`/Users/dxy_why/featureExtractTestData.csv`
as FeatureExtractInPlaceData;
train FeatureExtractInPlaceData as FeatureExtractInPlace.`/tmp/featureExtractInPlace`
where `inputCol`="doc"
;
```

The resule is saved in `/Users/dxy_why/tmp/featureExtractInPlace/data`
```
select  * from parquet.`/Users/dxy_why/tmp/featureExtractInPlace/data` 
```

Parameters:

|parameter|default|comments|
|:----|:----|:----|
|inputCol|"doc"||


```sql
register FeatureExtractInPlace.`/tmp/featureExtractInPlace` as predict;
```

Then their are some extra prediction functions available:


1. predict_phone  
1. predict_email 
1. predict_qqwechat 
1. predict_url the number of url
1. predict_pic  the number of pic
1. predict_blank  blank percentage
1. predict_chinese chinese percentage,
1. predict_english english percentage,
1. predict_number  number percentage,
1. predict_punctuation  punctuation percentage,
1. predict_mostchar mostchar percentage,
1. predict_length text length




### Discretizer

Discretizer for double column

#### Bucketizer

```sql

load libsvm.`/path/to/data/spark/data/mllib/sample_kmeans_data.txt` as data;

select vec_array(features)[0] as a, vec_array(features)[1] as b from data as table2;
save overwrite table2 as json.`/tmp/table2`;
train table2 as Discretizer.`/tmp/discretizer`

where method="bucketizer"

and `fitParam.0.inputCol`="a"

and `fitParam.0.splitArray`="-inf,0.0,1.0,inf"
and `fitParam.1.inputCol`="b"
and `fitParam.1.splitArray`="-inf,0.0,1.0,inf";
```

Parameters:

|parameter|default|comments|
|:----|:----|:----|
|method|bucketizer|support: bucketizer, quantile|
|fitParam.${index}.inputCols|None|double类型字段|
|fitParam.${index}.splitArray|None|bucket array，-inf ~ inf ，size should > 3，[x, y)|

Register model:

```sql
register Discretizer.`/tmp/discretizer` as jack;
```

Predict data:

```sql
select jack(array(a,b))[0] a,jack(array(a,b))[1] b, c from table2;
```

#### Quantile

Suppose `/tmp/discretizer3.data`:

```csv
id hour minute
0  18.0 1.0
1  19.0 20.0
2  8.0  33.0
3  5.0  23.4
4  2.2  44.5
```

示例

```sql

load csv.`/tmp/discretizer3.data` options header="True" as data;

select CAST(hour AS DOUBLE), CAST(minute AS DOUBLE) from data as table2;
train table2 as Discretizer.`/tmp/quantile`

where method="quantile"

and `fitParam.0.inputCol`="hour"
-- number of buckets
and `fitParam.0.numBuckets`="3"
and `fitParam.1.inputCol`="minute"
and `fitParam.1.numBuckets`="3";
```

Parameters:

|parameter|default|comments|
|:----|:----|:----|
|method|bucketizer|bucketizer, quantile|
|fitParam.${index}.inputCols|None|double type|
|fitParam.${index}.numBuckets|None|number of buckets|

Register:

```sql
register Discretizer.`/tmp/quantile` as jack;
```

Predict:

```sql
select hour, jack(array(hour, minute))[0] x, minute, jack(array(hour, minute))[1] y from table2 as result;
save overwrite result as json.`/tmp/result`;
```

Result: 

```json
{"hour":18.0,"x":2.0,"minute":1.0,"y":0.0}
{"hour":19.0,"x":2.0,"minute":20.0,"y":1.0}
{"hour":8.0,"x":1.0,"minute":33.0,"y":2.0}
{"hour":5.0,"x":1.0,"minute":23.4,"y":1.0}
{"hour":2.2,"x":0.0,"minute":44.5,"y":2.0}
```



### OpenCVImage

OpenCVImage only support image resize for now. When compile, you should add profile `opencv-support` to make StreamingPro
support OpenCVImage module.



```sql
-- get one url
select crawler_request_image("https://tpc.googlesyndication.com/simgad/10310202961328364833") as imagePath
as  images;

-- load images from hdfs file 
load image.`/training_set`
options
-- find image recursivelly 
recursive="true"

and dropImageFailures="true"

and sampleRatio="1.0"
-- read partiton number
and numPartitions="8"
-- process partion number
and repartitionNum="4"
-- image max size
and filterByteSize="2048576"
-- where to decode image
and enableDecode = "true"
as images;


-- select origin,width字段
-- select image.origin,image.width from images
-- as newimages;

train images as OpenCVImage.`/tmp/word2vecinplace`
where inputCol="imagePath"
and filterByteSize="2048576"
-- resize shape 
and shape="100,100,4"
;
load parquet.`/tmp/word2vecinplace/data`
as imagesWithResize;

-- convert vec_image to vector, the shape is [height * width * nChannels]
select vec_image(imagePath) as feature from imagesWithResize
as newTable;
```

Image schema:

```scala
StructType(
    StructField("origin", StringType, true) ::
      StructField("height", IntegerType, false) ::
      StructField("width", IntegerType, false) ::
      StructField("nChannels", IntegerType, false) ::
      StructField("mode", StringType, false) :: //OpenCV-compatible type: CV_8UC3 in most cases
      StructField("data", BinaryType, false) :: Nil) //bytes in OpenCV-compatible order: row-wise BGR in most cases
```


Register:

```sql
register OpenCVImage.`/tmp/word2vecinplace/` as jack;
```

Process:

```sql
select jack(crawler_request_image(imagePath)) as image from orginal_text_corpus
```

## VecMapInPlace

VecMapInPlace is used to convert Map[String,Double] column to Vector column。

Here the converting logical:
1. Collect all keys in all Map
2. Use StringIndex to convert key to number.
3. Create a vector space with the size of StringIndex.
4. Project the value in map to vector space.

Then you can use some normalization module to scale the vector features. 


```sql

train data as VecMapInPlace.`/tmp/jack`
where inputCol="a"

load parquet.`/tmp/jack/data` as newdata;

register VecMapInPlace.`/tmp/quantile` as jack;
select jack(map_value_int_to_double(map("wow",9)))
```


### JavaImage

JavaImage is a image library implemented by Java. You should enable it with profile opencv-support. 



具体用法：

```sql

select crawler_request_image("https://tpc.googlesyndication.com/simgad/10310202961328364833") as imagePath
as  images;


load image.`/training_set`
options

recursive="true"

and dropImageFailures="true"

and sampleRatio="1.0"

and numPartitions="8"

and repartitionNum="4"

and filterByteSize="2048576"

and enableDecode = "false"
as images;



-- select image.origin,image.width from images
-- as newimages;

train images as JavaImage.`/tmp/word2vecinplace`
where inputCol="imagePath"
and filterByteSize="2048576"

and shape="100,100,4"
-- scale method，default：AUTOMATIC
and method="SPEED"
-- scale mode，default：FIT_EXACT
and mode="AUTOMATIC"
;

load parquet.`/tmp/word2vecinplace/data`
as imagesWithResize;

select vec_image(imagePath) as feature from imagesWithResize
as newTable;
```

Parameters:

|parameter|default|comments|
|:----|:----|:----|
|method|AUTOMATIC|缩放方法|
|mode|FIT_EXACT|缩放模式|
|shape|none|width,height,channel，for example：100,100,3。channel will not work for now.|

method：

|值|说明|
|:----|:----|
|AUTOMATIC|自动,用于表明缩放的实现应该决定使用以获得最佳的期待在最少的时间缩放图像的|
|BALANCED|平衡,用于表明缩放的实现应该使用缩放操作的速度和质量之间的平衡|
|QUALITY|质量,用于表明缩放的实现应该尽其所能创造很好的效果越好|
|SPEED|用于表明缩放的实现的规模应该尽可能快并返回结果|
|ULTRA_QUALITY|用于表明缩放的实现应该超越的质量所做的工作，使图像看起来特别好的更多的处理时间成本|

mode：

|值|说明|
|:----|:----|
|AUTOMATIC|自动,用于表明缩放的实现应该计算所得的图像尺寸，通过查看图像的方向和发电比例尺寸，最佳为目标的宽度和高度，看到“给出更详细的scalr类描述图像的比例”|
|BEST_FIT_BOTH|最佳模式,用于表明缩放的实现应该计算，适合在包围盒的最大尺寸的图片，没有种植或失真，保持原来的比例|
|FIT_EXACT|精准模式,用适合的图像给不顾形象的比例精确的尺寸|
|FIT_TO_HEIGHT|用于表明缩放的实现应该计算尺寸的图像，最适合在给定的高度，无论图像的方向|
|FIT_TO_WIDTH|用于表明缩放的实现应该计算尺寸的图像，最适合在给定的宽度，无论图像的方向|

Image schema:

```scala
StructType(
    StructField("origin", StringType, true) ::
      StructField("height", IntegerType, false) ::
      StructField("width", IntegerType, false) ::
      StructField("nChannels", IntegerType, false) ::
      StructField("mode", StringType, false) ::
      StructField("data", BinaryType, false) :: Nil)
```


Register:

```sql
register JavaImage.`/tmp/word2vecinplace/` as jack;
```

Process:

```sql
select jack(crawler_request_image(imagePath)) as image from orginal_text_corpus
```

## SQLCorpusExplainInPlace

SQLCorpusExplainInPlace provides some statistic on corpus. 

```sql
train corpus as CorpusExplainInPlace.`/tmp/wow` where 
labelCol="label";

load parquet.`/tmp/wow/data` as result;
select * from result;
```

Here is the result.

```
+-----+----------+------------------+-------+-----+
|label|labelCount|            weight|percent|total|
+-----+----------+------------------+-------+-----+
|    1|         3|1.3333333333333333|   0.75|    4|
|    0|         1|               4.0|   0.25|    4|
+-----+----------+------------------+-------+-----+
```


## RateSampler
 
With RateSampler, you can easily split the data to train/test set. 
A new column `__split__` will be created once you apply this module the corpus.

Note that RateSample will split every category by rate you specify. 
 

```sql

train lwys_corpus_final_format as RateSampler.`${traning_dir}/ratesampler` 
where labelCol="label"
and sampleRate="0.9,0.1";

load parquet.`${traning_dir}/ratesampler` as data2;

select * from data2 where __split__=1
as validateTable;

select * from data2 where __split__=0
as trainingTable;
```

When you know your dataset is small, you can split more accurately with  isSplitWithSubLabel enabled.
```
and isSplitWithSubLabel="true"
```
 

## ModelExplainInPlace

ModelExplainInPlace is used to load sklearn or sparkmllib model，and show the parameters in the model.

Load sklearn model:
```
train traindataframe as ModelExplainInPlace.`tmp/modelExplainInPlace/` 
where `systemParam.pythonPath`="python"
and `systemParam.pythonVer`="2.7"
// 模型路径
and `modelPath`="/tmp/svm.pickle"
```

Load sparkmllib model:
```
train traindataframe as ModelExplainInPlace.`tmp/modelExplainInPlace/` 
where `modelPath`="/tmp/model"
and `modelType`="sparkmllib"
```

The model parameters is saved in `/tmp/modelExplainInPlace/data`.

```
select  * from parquet.`/tmp/modelExplainInPlace/data` 
```

Parameters:

|parameter|default|comments|
|:----|:----|:----|
|modelPath|""|model path|


### Word2ArrayInPlace


Word2ArrayInPlace can load Word2VecInPlace,TfIdfInPlace model, and convert the text to number of sequence.




```sql
load parquet.`/tmp/tfidf/df`
as orginal_text_corpus;

train orginal_text_corpus as Word2VecInPlace.`/tmp/word2vecinplace`
where inputCol="content"
and split=""
;

-- here orginal_text_corpus can be any table exits.
train orginal_text_corpus as Word2ArrayInPlace.`/tmp/word2arrayinplace`
--  the path of Word2VecInPlace,TfIdfInPlace
where modelPath="/tmp/word2vecinplace"
-- and wordvecPaths=""
;
-- register 
register Word2ArrayInPlace.`/tmp/word2arrayinplace`
as word2array_predict;
```


### RowMatrix


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

### CommunityBasedSimilarityInPlace


```sql
train tab;e as CommunityBasedSimilarityInPlace.`/tmp/zhuhl_rm_model`
-- 设置需要过滤的边的阈值以及联通子图的大小 
where minSimilarity="0.7"
and minCommunitySize="2"
and minCommunityPercent="0.2"

-- 接受的数据rowNum和columnNum是两个节点的id,edgeValue是节点的边的值
and "rowNum"="i"
and "columnNum"="j"
and "edgeValue"="v"
;

load parquet.`/tmp/zhuhl_rm_model/data` as result;
select * from result as output;

-- 结果如下：
 +-----+---------+
 |group|vertexIds|
 +-----+---------+
 |3    |[3, 7, 5]|
 +-----+---------+
```

### RawSimilarInPlace

RawSimilarInPlace is used to compute text similarity.

具体用法：

```sql
load parquet.`/tmp/tfidf/df`
as word2vec_corpus;


train word2vec_corpus as Word2VecInPlace.`/tmp/word2vecinplace`
where inputCol="content"
;


load parquet.`/tmp/raw` as data；


train data as RawSimilarInPlace.`/tmp/rawsimilar` where
modelPath="/tmp/william/tmp/word2vecinplace"
-- the splitter of sentensce，default value is "。", you can provide multi values,eg.",。；"
and sentenceSplit=",。"
-- for now it's only support Word2VecInplace
and modelType="Word2VecInplace"
and inputCol="features"
and labelCol="label";

register RawSimilarInPlace.`/tmp/rawsimilar` as rs_predict;
-- 0.8为相似度比例阀值,得到与文章相似度高于阀值的文章label,结果类型为Map[Long,Double]
select rs_predict(label,0.8) from data;
```

### WaterMarkInPlace

WaterMarkInPlace is used to set watermark for stream dataframe.


```sql
select cast(key as string) as k,cast(timestamp as timestamp) as ts  from newkafkatable1 as table21;

-- as后的表a任意写，不冲突就行，但必须填
register WaterMarkInPlace.`table21` as a
options eventTimeCol="ts"
and delayThreshold="1 seconds";

select count(*) as num from table21
group by window(ts,"20 seconds","10 seconds")
as table22;
```


### DicOrTableToArray

Fancy tricks. DicOrTableToArray can convert dictionary file or table to array.
you can call a user-defined-function to get a array.


```sql
select explode(k) as word from (select p("dic1") as k)
as words_table;

select lower(word) from words_table
as array_table;

train newdata as DicOrTableToArray.`/tmp/model2` where 
`table.paths`="array_table" 
and `table.names`="dic2";

register DicOrTableToArray.`/tmp/model2` as p2;

-- where you can get dic2 array.
select p2("dic2")  as k

```



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


### SendMessage
You can use this module to send email.
Note that we now only support smtp prototcol.


```
-- 设置变量a为执行sql的结果
-- set a = `select "email content"` options type = "sql";
set a = '''
here is the email content
''';

set smtp-ip-address="127.0.0.1";

-- 设置smtp的ip地址
set smtp-ip-address = "localhost";
select "${a}" as content as data;

-- you also can load email content from file.
-- load json.`/tmp/a.json` as data;

train data as SendMessage.`_`
-- for now only support mail.
where method="mail"
-- receivers
and to = "xxxx@xxx.com"
-- email title
and subject = "这是邮件标题"
-- email server
and smtpHost = "${smtp-ip-address}";
```

### JDBC

JDBC module enable MLSQL operate SQL DB directly.
Notice that, for now ,The SQL executed by JDBC module is runed on driver.

```sql
connect jdbc where
driver="com.mysql.jdbc.Driver"
and url="jdbc:mysql://127.0.0.1:3306/wow"
and driver="com.mysql.jdbc.Driver"
and user="---"
and password="----"
as mysql1;

select 1 as t as fakeTable;

train fakeTable as JDBC.`mysql1` where
`driver-statement-0`="drop table test1";
```
