load parquet.`/tmp/tfidf/df`
as orginal_text_corpus;

-- 把文本字段转化为tf/idf向量,可以自定义词典
train orginal_text_corpus as TfIdfInPlace.`/tmp/tfidfinplace`
where inputCol="content"
-- 分词相关配置
and ignoreNature="true"
-- 停用词路径
and stopWordPath="/tmp/tfidf/stopwords"
-- 高权重词路径
and priorityDicPath="/tmp/tfidf/prioritywords"
and dicPaths="/tmp/tfidf/dics"
-- 高权重词加权倍数
and priority="5.0"
and nGrams="2"
-- and split=""
;

register TfIdfInPlace.`/tmp/tfidfinplace` as jack;