```sql
set traning_dir = "/tmp/lwys_corpus";

-- 加载数据
load csv.`/Users/allwefantasy/Downloads/lwys_corpus` options header="true" and delimiter="\t" and quote="'"
as lwys_corpus;

select cut as features,cast(sid as int) as label from lwys_corpus
as orginal_text_corpus;

-- 把文本字段转化为tf/idf向量
train orginal_text_corpus as TfIdfInPlace.`${traning_dir}/tfidf` 
where inputCol="features" 
and `dic.paths`=".....";

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
and validateTable="validateTable"
and `systemParam.pythonPath`="python"
and `systemParam.pythonVer`="2.7"
;
```