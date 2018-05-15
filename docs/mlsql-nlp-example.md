
### 基于SKLearn算法
环境要求请参看 [MLSQL-单机算法 based on SKLearn](https://github.com/allwefantasy/streamingpro/blob/master/docs/mlsql-sklearn.md)

```sql

set traning_dir = "/tmp/lwys_corpus";

-- 加载数据
load csv.`/Users/allwefantasy/Downloads/lwys_corpus` options header="true" and delimiter="\t" and quote="'"
as lwys_corpus;

select cut as features,cast(sid as int) as label from lwys_corpus
as orginal_text_corpus;

-- 把文本字段转化为tf/idf向量,可以自定义词典
train orginal_text_corpus as TfIdfInPlace.`${traning_dir}/tfidf` 
where inputCol="features" 

-- 分词的字典路径，支持多个
and `dic.paths`="....."

-- 停用词路径
and stopWordPath="..."

-- 高权重词路径
and priorityDicPath="...."

-- 高权重词加权倍数
and priority="5.0"

-- 采用nGrams组合特征，混合n=1,n=2,n=3 
and nGrams="2,3"
;

load parquet.`${traning_dir}/tfidf` 
as lwys_corpus_with_featurize;

-- 把label转化为递增数字
train lwys_corpus_with_featurize StringIndex.`${traning_dir}/si` 
where inputCol="label";

register StringIndex.`${traning_dir}/si` as predict;

select predict(label) as label,features as features from lwys_corpus_with_featurize 
as lwys_corpus_final_format;

-- 切分训练集、验证集，该算法会保证每个分类都是按比例切分。
train lwys_corpus_final_format as RateSampler.`${traning_dir}/ratesampler` 
where labelCol="label"
and sampleRate="0.9,0.1";

load parquet.`${traning_dir}/ratesampler` as data2;

select * from data2 where __split__=1
as validateTable;

select * from data2 where __split__=0
as trainingTable;

-- 训练，可以配置多个模型同时进行训练

train trainingTable as SKLearn.`${traning_dir}/model`  
where `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and `kafkaParam.topic`="test"
and `kafkaParam.group_id`="g_test-1"
and  `fitParam.0.batchSize`="300"
and  `fitParam.0.labelSize`="41"
and  `fitParam.0.alg`="RandomForestClassifier"
and  `fitParam.1.batchSize`="300"
and  `fitParam.1.labelSize`="41"
and  `fitParam.1.alg`="SVM"
and validateTable="validateTable"
and `systemParam.pythonPath`="python"
and `systemParam.pythonVer`="2.7"
;

-- 注册模型，得到预测udf函数
register SKLearn.`${traning_dir}/model` as sk_predict;

-- 对验证机进行预测（只是展示如何使用预测udf函数）
select sk_predict(features) as res from validateTable
as result;

```

### 基于TensorFlow做文本分类

```sql
set traning_dir = "/tmp/lwys_corpus";

-- 加载数据
load csv.`/Users/allwefantasy/Downloads/lwys_corpus` options header="true" and delimiter="\t" and quote="'"
as lwys_corpus;

select cut as features,cast(sid as int) as label from lwys_corpus
as orginal_text_corpus;

-- 把文本字段转化为tf/idf向量,可以自定义词典
train orginal_text_corpus as Word2VecInPlace.`${traning_dir}/tfidf` 
where inputCol="features";

load parquet.`${traning_dir}/tfidf` 
as lwys_corpus_with_featurize;

set featureSize = "select size(features) as featureSize from lwys_corpus_with_featurize" options type="sql";

-- 把label转化为递增数字
train lwys_corpus_with_featurize StringIndex.`${traning_dir}/si` 
where inputCol="label";

register StringIndex.`${traning_dir}/si` as predict;

select predict(label) as label,features as features from lwys_corpus_with_featurize 
as lwys_corpus_final_format;

-- 切分训练集、验证集，该算法会保证每个分类都是按比例切分。
train lwys_corpus_final_format as RateSampler.`${traning_dir}/ratesampler` 
where labelCol="label"
and sampleRate="0.9,0.1";

load parquet.`${traning_dir}/ratesampler` as data2;

select * from data2 where __split__=1
as validateTable;

select * from data2 where __split__=0
as trainingTable;

-- 训练，可以配置多个模型同时进行训练

train newdata as TensorFlow.`/tmp/model2`
where `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and   `kafkaParam.topic`="test"
and   `kafkaParam.group_id`="g_test-1"
and   `kafkaParam.reuse`="false"
and   `fitParam.0.layerGroup`="300,100"
and   `fitParam.0.epochs`="1"
and   `fitParam.0.batchSize`="32"
and   `fitParam.0.featureSize`="${featureSize}"
and   `fitParam.0.labelSize`="41"
and   `fitParam.0.alg`="FCClassify"
and   validateTable="validateTable"
and   `systemParam.pythonPath`="python"
and `systemParam.pythonVer`="2.7"

register  TensorFlow.`/tmp/model2`  as tf_predict;

```