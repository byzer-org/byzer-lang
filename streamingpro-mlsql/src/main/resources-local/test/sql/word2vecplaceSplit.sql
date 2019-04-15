load parquet.`/tmp/tfidf/df`
as orginal_text_corpus;

-- 把文本字段转化为tf/idf向量,可以自定义词典
train orginal_text_corpus as Word2VecInPlace.`/tmp/word2vecinplace`
where inputCol="content"
-- 分词相关配置
and ignoreNature="true"
and resultFeature="${resultFeature}"
and wordvecPaths="${wordvecPaths}"
and split="${split}"
;

register Word2VecInPlace.`/tmp/word2vecinplace` as jack;