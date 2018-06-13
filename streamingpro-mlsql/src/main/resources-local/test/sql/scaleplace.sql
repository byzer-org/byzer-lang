-- 把文本字段转化为tf/idf向量,可以自定义词典
train orginal_text_corpus as ScalerInPlace.`/tmp/scaler`
where inputCols="a,b"
-- 使用是什么缩放方法
and scaleMethod="min-max"
-- 是否自动修正异常值
and removeOutlierValue="false"
;

register ScalerInPlace.`/tmp/scaler` as jack;