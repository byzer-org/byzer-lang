
- [**高阶数据预处理模型**](#高阶开箱即用数据预处理模型)
  - [TfIdfInPlace](#tfidfinplace)
  - [Word2VecInPlace](#word2vecinplace)
  - [ScalerInPlace](#scalerinplace)
  - [NormalizeInPlace](#normalizeinplace)
  - [Discretizer](#discretizer)
    - [bucketizer](#bucketizer方式)
    - [quantile](#quantile方式)
  - [OpenCVImage](#opencvimage)
  - [JavaImage](#javaimage)
  - [TokenExtract / TokenAnalysis](#tokenextract--tokenanalysis)
  - [RateSampler](#ratesampler)
- [**低阶数据预处理模型**](#低阶特定小功能点数据预处理模型)
  - [Word2vec](#word2vec)
  - [StringIndex](#stringindex)
  - [TfIdf](#tfidf)
  - [StandardScaler](#standardscaler)
  - [DicOrTableToArray](#dicortabletoarray)

---

### 高阶（开箱即用）数据预处理模型

### TfIdfInPlace

TfIdfInPlace是一个较为复杂的预处理模型。首先你需要开启[ansj分词支持](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-analysis.md)。
TfIdfInPlace能够把一个raw文本转化为一个向量。具体流程如下：

1. 分词（可以指定自定义词典）
2. 过滤停用词
3. ngram特征组合
4. 字符转化为数字
5. 计算idf/tf值
6. 记在权重高的词汇，并且给对应特征加权。


具体用法：

```sql
-- 把文本字段转化为tf/idf向量,可以自定义词典
train orginal_text_corpus as TfIdfInPlace.`/tmp/tfidfinplace`
where inputCol="content"
-- 分词相关配置
and ignoreNature="true"
and dicPaths="...."
-- 停用词路径
and stopWordPath="/tmp/tfidf/stopwords"
-- 高权重词路径
and priorityDicPath="/tmp/tfidf/prioritywords"
-- 高权重词加权倍数
and priority="5.0"
-- ngram 配置
and nGram="2,3"
;

load parquet.`/tmp/tfidf/data` 
as lwys_corpus_with_featurize;
```

lwys_corpus_with_featurize 中的content字段已经是向量化的了，可以直接进行后续的算法训练。

对于陌生数据，我们可以注册该模型从而能够将陌生数据也转化为向量：

```sql
register TfIdfInPlace.`/tmp/tfidf/` as tfidf;
```

tfidf函数接受一个字符串，返回一个向量。使用如下：

```sql
select tfidf(content) from sometable
```

这意味着你注册后就可以在流式计算或者批处理里直接使用这个函数。

当然，我们还可能希望将TfIdfInPlace模型部署在一个API服务里，
可参考[MLSQL 模型部署](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-model-deploy.md)

### Word2VecInPlace

Word2VecInPlace是一个较为复杂的预处理模型。首先你需要开启[ansj分词支持](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-analysis.md)。
Word2VecInPlace能够把一个raw文本转化为一个向量。具体流程如下：

1. 分词（可以指定自定义词典）
2. 过滤停用词
3. 字符转化为数字
4. 把数字转化为向量
5. 返回一维或者二维数组

目前主要给深度学习做NLP使用，譬如卷积网络等。

具体用法：

```sql
load parquet.`/tmp/tfidf/df`
as orginal_text_corpus;

-- 把文本字段转化为词向量数组,可以自定义词典
train orginal_text_corpus as Word2VecInPlace.`/tmp/word2vecinplace`
where inputCol="content"
-- 分词相关配置
and ignoreNature="true"
-- 停用词路径
and stopWordPath="/tmp/tfidf/stopwords"
-- 把结果展开为一维向量
and flatFeature="true"
;
load parquet.`/tmp/word2vecinplace/data` 
as lwys_corpus_with_featurize;
```

lwys_corpus_with_featurize 中的content字段已经是向量化的了，可以直接进行后续的算法训练。

对于陌生数据，我们可以注册该模型从而能够将陌生数据也转化为向量：

```sql
register Word2VecInPlace.`/tmp/word2vecinplace` as word2vec;
```

word2vec函数接受一个字符串，返回一个向量。使用如下：

```sql
select word2vec(content) from sometable
```

这意味着你注册后就可以在流式计算或者批处理里直接使用这个函数。

当然，我们还可能希望将Word2VecInPlace模型部署在一个API服务里，
可参考[MLSQL 模型部署](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-model-deploy.md)


### ScalerInPlace

特征尺度变换，这是对double类型字段做特征工程的一个算法。使用方法如下：

```sql
-- 把文本字段转化为tf/idf向量,可以自定义词典
train orginal_text_corpus as ScalerInPlace.`/tmp/scaler`
where inputCols="a,b"
-- 使用是什么缩放方法
and scaleMethod="min-max"
-- 是否自动修正异常值
and removeOutlierValue="false"
;

--得到特征化的数据
load parquet.`/tmp/scaler/data` 
as featurize_table;

```

参数使用说明：

|参数|默认值|说明|
|:----|:----|:----|
|inputCols|None|double类型字段列表，用逗号分隔|
|scaleMethod|log2|目前支持的有：minx-max,log2,logn,log10,sqrt,abs等|
|removeOutlierValue|false|是否自动去掉异常点，使用中位数替换|


对于新数据，你首先需要注册下之前训练产生的模型：

```sql
register ScalerInPlace.`/tmp/scaler` as jack;
```

接着你便可以使用该模型对新数据做处理了：

```sql
select jack(array(a,b))[0] a,jack(array(a,b))[1] b, c from orginal_text_corpus
```

## NormalizeInPlace

对double类型特征进行标准化/规范化。


```
-- 把文本字段转化为tf/idf向量,可以自定义词典
train orginal_text_corpus as NormalizeInPlace.`/tmp/scaler2`
where inputCols="a,b"
-- 使用是什么缩放方法
and method="standard"
-- 是否自动修正异常值
and removeOutlierValue="false"
;

register NormalizeInPlace.`/tmp/scaler2` as jack;
```

参数使用说明：

|参数|默认值|说明|
|:----|:----|:----|
|inputCols|None|double类型字段列表，用逗号分隔，或者单独的array&lt;double&gt;列|
|method|standard|目前支持的有：standard,p-norm等|
|removeOutlierValue|false|是否自动去掉异常点，使用中位数替换|

对于新数据，你首先需要注册下之前训练产生的模型：

```sql
register NormalizeInPlace.`/tmp/scaler2` as jack;
```

接着你便可以使用该模型对新数据做处理了：

```sql
select jack(array(a,b))[0] a,jack(array(a,b))[1] b, c from orginal_text_corpus
```

### Discretizer

对double类型的连续值特征进行离散化。

#### bucketizer方式

```sql
-- 加载训练数据
load libsvm.`/path/to/data/spark/data/mllib/sample_kmeans_data.txt` as data;
-- 类型转换
select vec_array(features)[0] as a, vec_array(features)[1] as b from data as table2;
save overwrite table2 as json.`/tmp/table2`;
train table2 as Discretizer.`/tmp/discretizer`
-- 定义采用哪种散列化方式
where method="bucketizer"
-- 输入列
and `fitParam.0.inputCol`="a"
-- 分裂数组
and `fitParam.0.splitArray`="-inf,0.0,1.0,inf"
and `fitParam.1.inputCol`="b"
and `fitParam.1.splitArray`="-inf,0.0,1.0,inf";
```

参数使用说明：

|参数|默认值|说明|
|:----|:----|:----|
|method|bucketizer|散列化方式，支持bucketizer, quantile|
|fitParam.${index}.inputCols|None|double类型字段|
|fitParam.${index}.splitArray|None|分裂数组，-inf表示负无穷大，inf表示正无穷大，分裂区间不能小于3，每个区间的范围是左闭右开，即[x, y)|

对于新数据，你首先需要注册下之前训练产生的模型：

```sql
register Discretizer.`/tmp/discretizer` as jack;
```

接着你便可以使用该模型对新数据做处理了：

```sql
select jack(array(a,b))[0] a,jack(array(a,b))[1] b, c from table2;
```

#### quantile方式

假设/tmp/discretizer3.data数据如下：

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
-- 加载训练集
load csv.`/tmp/discretizer3.data` options header="True" as data;
-- 类型转换
select CAST(hour AS DOUBLE), CAST(minute AS DOUBLE) from data as table2;
train table2 as Discretizer.`/tmp/quantile`
-- 散列化方式
where method="quantile"
-- 输入列
and `fitParam.0.inputCol`="hour"
-- number of buckets
and `fitParam.0.numBuckets`="3"
and `fitParam.1.inputCol`="minute"
and `fitParam.1.numBuckets`="3";
```

参数使用说明：

|参数|默认值|说明|
|:----|:----|:----|
|method|bucketizer|散列化方式，支持bucketizer, quantile|
|fitParam.${index}.inputCols|None|double类型字段|
|fitParam.${index}.numBuckets|None|number of buckets|

对于新数据，你首先需要注册下之前训练产生的模型：

```sql
register Discretizer.`/tmp/quantile` as jack;
```

接着你便可以使用该模型对新数据做处理了：

```sql
select hour, jack(array(hour, minute))[0] x, minute, jack(array(hour, minute))[1] y from table2 as result;
save overwrite result as json.`/tmp/result`;
```

散列化结果:
```json
{"hour":18.0,"x":2.0,"minute":1.0,"y":0.0}
{"hour":19.0,"x":2.0,"minute":20.0,"y":1.0}
{"hour":8.0,"x":1.0,"minute":33.0,"y":2.0}
{"hour":5.0,"x":1.0,"minute":23.4,"y":1.0}
{"hour":2.2,"x":0.0,"minute":44.5,"y":2.0}
```


### OpenCVImage

OpenCVImage模块主要是对图像做处理。第一版仅仅能够做resize动作。后面会持续扩充功能。

因为引入OpenCV模块jar包体积会很大，所以编译项目时，需要加上 -Popencv-support 才能使得该功能生效。

具体用法：

```sql
-- 抓取一张图片
select crawler_request_image("https://tpc.googlesyndication.com/simgad/10310202961328364833") as imagePath
as  images;

-- 或者你可能因为训练的原因，需要加载一个图片数据集 该表只有一个字段image,但是image是一个复杂字段，其中origin 带有路径信息。
load image.`/training_set`
options
-- 递归目录查找图片
recursive="true"
-- 丢弃解析失败的图片
and dropImageFailures="true"
-- 采样率
and sampleRatio="1.0"
-- 读取图片线程数
and numPartitions="8"
-- 处理图片线程数
and repartitionNum="4"
-- 单张图片最大限制
and filterByteSize="2048576"
-- 可以禁止对图片进行解析，避免占用过大资源，让后续的OpenCVImage来解析
and enableDecode = "true"
as images;


-- 比如 选择origin,width字段
-- select image.origin,image.width from images
-- as newimages;

train images as OpenCVImage.`/tmp/word2vecinplace`
where inputCol="imagePath"
and filterByteSize="2048576"
-- 宽度和高度重新设置为100
and shape="100,100,4"
;
load parquet.`/tmp/word2vecinplace/data`
as imagesWithResize;

-- 通过vec_image 可以将图片转化为一个一维数组,结构为[height * width * nChannels]
select vec_image(imagePath) as feature from imagesWithResize
as newTable;
```

图片字段有一个单独的数据格式：

```scala
StructType(
    StructField("origin", StringType, true) ::
      StructField("height", IntegerType, false) ::
      StructField("width", IntegerType, false) ::
      StructField("nChannels", IntegerType, false) ::
      StructField("mode", StringType, false) :: //OpenCV-compatible type: CV_8UC3 in most cases
      StructField("data", BinaryType, false) :: Nil) //bytes in OpenCV-compatible order: row-wise BGR in most cases
```


对于新图片，你首先需要注册下之前训练产生的模型：

```sql
register OpenCVImage.`/tmp/word2vecinplace/` as jack;
```

接着你便可以使用该模型对新数据做处理了：

```sql
select jack(crawler_request_image(imagePath)) as image from orginal_text_corpus
```


### JavaImage

JavaImage模块主要是对图像做处理。第一版仅仅能够做resize动作。后面会持续扩充功能。

因为目前和OpenCV集成在一起，引入OpenCV模块jar包体积会很大，所以编译项目时，需要加上 -Popencv-support 才能使得该功能生效。

具体用法：

```sql
-- 抓取一张图片
select crawler_request_image("https://tpc.googlesyndication.com/simgad/10310202961328364833") as imagePath
as  images;

-- 或者你可能因为训练的原因，需要加载一个图片数据集 该表只有一个字段image,但是image是一个复杂字段，其中origin 带有路径信息。
load image.`/training_set`
options
-- 递归目录查找图片
recursive="true"
-- 丢弃解析失败的图片
and dropImageFailures="true"
-- 采样率
and sampleRatio="1.0"
-- 读取图片线程数
and numPartitions="8"
-- 处理图片线程数
and repartitionNum="4"
-- 单张图片最大限制
and filterByteSize="2048576"
-- 禁止对图片进行解析，用openCV算法可以设置为true
and enableDecode = "false"
as images;


-- 比如 选择origin,width字段
-- select image.origin,image.width from images
-- as newimages;

train images as JavaImage.`/tmp/word2vecinplace`
where inputCol="imagePath"
and filterByteSize="2048576"
-- 宽度和高度重新设置为100,第三个数字4在此算法不生效
and shape="100,100,4"
-- 缩放方法，默认：AUTOMATIC
and method="SPEED"
-- 缩放模式，默认：FIT_EXACT
and mode="AUTOMATIC"
;

load parquet.`/tmp/word2vecinplace/data`
as imagesWithResize;

-- 通过vec_image 可以将图片转化为一个一维数组,结构为[height * width * nChannels]
select vec_image(imagePath) as feature from imagesWithResize
as newTable;
```
参数使用说明：

|参数|默认值|说明|
|:----|:----|:----|
|method|AUTOMATIC|缩放方法|
|mode|FIT_EXACT|缩放模式|
|shape|none|width,height,channel，例如：100,100,3。channel不生效|

method说明：

|值|说明|
|:----|:----|
|AUTOMATIC|自动,用于表明缩放的实现应该决定使用以获得最佳的期待在最少的时间缩放图像的|
|BALANCED|平衡,用于表明缩放的实现应该使用缩放操作的速度和质量之间的平衡|
|QUALITY|质量,用于表明缩放的实现应该尽其所能创造很好的效果越好|
|SPEED|用于表明缩放的实现的规模应该尽可能快并返回结果|
|ULTRA_QUALITY|用于表明缩放的实现应该超越的质量所做的工作，使图像看起来特别好的更多的处理时间成本|

mode说明：

|值|说明|
|:----|:----|
|AUTOMATIC|自动,用于表明缩放的实现应该计算所得的图像尺寸，通过查看图像的方向和发电比例尺寸，最佳为目标的宽度和高度，看到“给出更详细的scalr类描述图像的比例”|
|BEST_FIT_BOTH|最佳模式,用于表明缩放的实现应该计算，适合在包围盒的最大尺寸的图片，没有种植或失真，保持原来的比例|
|FIT_EXACT|精准模式,用适合的图像给不顾形象的比例精确的尺寸|
|FIT_TO_HEIGHT|用于表明缩放的实现应该计算尺寸的图像，最适合在给定的高度，无论图像的方向|
|FIT_TO_WIDTH|用于表明缩放的实现应该计算尺寸的图像，最适合在给定的宽度，无论图像的方向|

图片字段有一个单独的数据格式：

```scala
StructType(
    StructField("origin", StringType, true) ::
      StructField("height", IntegerType, false) ::
      StructField("width", IntegerType, false) ::
      StructField("nChannels", IntegerType, false) ::
      StructField("mode", StringType, false) ::
      StructField("data", BinaryType, false) :: Nil)
```


对于新图片，你首先需要注册下之前训练产生的模型：

```sql
register JavaImage.`/tmp/word2vecinplace/` as jack;
```

接着你便可以使用该模型对新数据做处理了：

```sql
select jack(crawler_request_image(imagePath)) as image from orginal_text_corpus
```



## TokenExtract / TokenAnalysis

[TokenExtract / TokenAnalysis](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-analysis.md)


## RateSampler
 
 对样本里的每个分类按比例进行切分。之后会对数据生成一个新的字段__split__, 该字段为int类型。比如下面的例子中，
 0.9对应的数据集为0,0.1对应的数据集为1。

```sql
-- 切分训练集、验证集，该算法会保证每个分类都是按比例切分。
train lwys_corpus_final_format as RateSampler.`${traning_dir}/ratesampler` 
where labelCol="label"
and sampleRate="0.9,0.1";

load parquet.`${traning_dir}/ratesampler` as data2;

select * from data2 where __split__=1
as validateTable;

select * from data2 where __split__=0
as trainingTable;
```

## 低阶（特定小功能点）数据预处理模型

### Word2vec

假设"/tmp/test.csv"内容为：

```
body
a b c
a d m
j d c
a b c
b b c
```

通过Word2vec可以为里面每个字符计算一个向量。

示例:

```sql
load csv.`/tmp/test.csv` options header="True" as ct;

select split(body," ") as words from ct as new_ct;

train new_ct as word2vec.`/tmp/w2v_model` where inputCol="words" and minCount="0";

register word2vec.`/tmp/w2v_model` as w2v_predict;

select words[0] as w, w2v_predict(words[0]) as v from new_ct as result;

save overwrite result as json.`/tmp/result`;

```

### StringIndex

StringIndex可以给每个词汇生成一个唯一的ID。

```sql
load csv.`/tmp/abc.csv` options header="True" as data;
select explode(split(body," ")) as word from data as new_dt;
train new_dt as StringIndex.`/tmp/model` where inputCol="word";
register StringIndex.`/tmp/model` as predict;
select predict_r(1)  from new_dt as result;
save overwrite result as json.`/tmp/result`;
```

除了你注册的predict函数以外，StringIndex会隐式给你生成一些函数，包括：

* predict    参数为一个词汇，返回一个数字
* predict_r  参数为一个数字，返回一个词
* predict_array 参数为词汇数组，返回数字数组
* predict_rarray 参数为数字数组，返回词汇数组

### TfIdf

```sql
--  加载文本数据
load csv.`/tmp/test.csv` options header="True" 
as zhuhl_ct;

--分词
select split(body," ") as words from zhuhl_ct 
as zhuhl_new_ct;

-- 把文章转化为数字序列，因为tfidf模型需要数字序列

train word_table as StringIndex.`/tmp/zhuhl_si_model` where 
inputCol="word" and handleInvalid="skip";

register StringIndex.`/tmp/zhuhl_si_model` as zhuhl_si_predict;

select zhuhl_si_predict_array(words) as int_word_seq from zhuhl_new_ct
as int_word_seq_table;

-- 训练一个tfidf模型
train int_word_seq_table as TfIdf.`/tmp/zhuhl_tfidf_model` where 
inputCol="words"
and numFeatures="100" 
and binary="true";

--注册tfidf模型
register TfIdf.`/tmp/zhuhl_tfidf_model` as zhuhl_tfidf_predict;

--将文本转化为tf/idf向量
select zhuhl_tfidf_predict(int_word_seq) as features from int_word_seq_table
as lda_data;

```

通过tf/idf模型预测得到的就是向量，可以直接被其他算法使用。和libsvm 格式数据一致。



### StandardScaler

```sql
load libsvm.`/Users/allwefantasy/Softwares/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;

train data as StandardScaler.`/tmp/kk`where inputCol="features";
register StandardScaler.`/tmp/kk` as predict;
select predict(features) as k from data;
```

### DicOrTableToArray

DicOrTableToArray 是一个很有意思的模型，
很多场景我们需要把表或者字典转化为一个数组类型的字段，那么这个模型可以提供这样的功能。
他会把该数组表示成一个UDF函数，之后通过该UDF函数在任何SQL语句里获取对应的数组。

示例：


有文件/tmp/test.txt文件如下：

```
content
a b c
b c d
e f g
```

有字典：/tmp/abc.txt:

```
a
b
c
```



```sql
load csv.`/tmp/test.txt` options header="True" as data; 

select split(content," ") as words from data 
as newdata;

train newdata as DicOrTableToArray.`/tmp/model` where 
`dic.paths`="/tmp/abc.txt" 
and `dic.names`="dic1";

register DicOrTableToArray.`/tmp/model` as p;
```

dic.paths 指定字典的路径，多个按','分割
dic.names 指定之巅的名称，多个按','分割

table.paths 指定表，多个按','分割
table.names 指定之巅的名称，多个按','分割

之后就可以注册模型了，假设该函数为p，调用该函数并且传入字典名称，你可以随时一个字符串数组，
并且通过array_slice,array_index等数组相关的函数进行操作。有的时候，我们希望对数组里的
元素进行一一解析，一个简单的技巧是：

我们先把数组转化为表：

```sql
select explode(k) as word from (select p("dic1") as k)
as words_table;

```

接着就可以对每个元素做处理了:

```sql
select lower(word) from words_table
as array_table;
```

处理完成后，我们希望能用相同的方式，比如调用一个函数，就能随时获取一个数组，这个时候我们复用下前面的训练：


```sql
train newdata as DicOrTableToArray.`/tmp/model2` where 
`table.paths`="array_table" 
and `table.names`="dic2";

register DicOrTableToArray.`/tmp/model2` as p2;
```

现在，我们可以通过调用p2获取处理后的数组了：

```
select p2("dic2")  as k
```





