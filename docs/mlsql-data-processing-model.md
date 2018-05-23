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





