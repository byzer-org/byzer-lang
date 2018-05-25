-- 把文本字段转化为tf/idf向量,可以自定义词典
train orginal_text_corpus as NormalizeInPlace.`/tmp/scaler2`
where inputCols="a,b"
-- 使用是什么缩放方法
and method="${method}"
-- 是否自动修正异常值
and removeOutlierValue="false"
;

register NormalizeInPlace.`/tmp/scaler2` as jack;