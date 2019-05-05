## RawSimilarInPlace

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