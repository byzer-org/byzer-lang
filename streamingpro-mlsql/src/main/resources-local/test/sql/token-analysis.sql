-- 待处理文本
select "天了噜我是天才" as words ,"1" as id
as newdata;

-- 指定词典，然后TokenExtract 会在/tmp/model生成一个parquet文件，包含id和keywords两个字段。
-- id 是你指定的内容的唯一标号，需要是字符串类型。
-- keywords则是你指定的文本列里抽取出来的在字典中的词汇。

train newdata as TokenAnalysis.`/tmp/model` where
`dic.paths`="/tmp/abc.txt"
and idCol="id"
and parser="org.ansj.splitWord.analysis.DicAnalysis"
and inputCol="words";

load parquet.`/tmp/model` as tb;