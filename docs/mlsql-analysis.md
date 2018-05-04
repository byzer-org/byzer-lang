## 如何在MLSQL中运用分词抽词工具

该功能主要依赖于开源项目[ansj_seg](https://github.com/NLPchina/ansj_seg)。
依赖的jar包为：

1. ansj_seg-5.1.6.jar
2. nlp-lang-1.7.8.jar 

启动MLSQL时，需要通过--jars带上这两个jar包。这两个包打包非常简单，可以clone后简单用，执行
如下指令即可：

```shell
mvn -DskipTests package 
```

因为要能够随时指定或者变更词典，所以以训练模型的方式来做模拟分词抽词相关的工作。未来不排除通过udf函数的方式，
这样使用上会更简洁些。

### 分词 - TokenAnalysis

Ansj的词典格式为：

```
词\t词性\t权重频次int类型
```

使用方式：

```sql
-- 待处理文本
select "天了噜我是天才" as words ,"1" as id
as newdata;

-- 指定词典，TokenAnalysis 会在/tmp/model生成一个parquet文件，包含id和keywords两个字段。
-- id 是你指定的内容的唯一标号，需要是字符串类型。
-- keywords 为分词结果，是一个数组

train newdata as TokenAnalysis.`/tmp/model` where
`dic.paths`="/tmp/abc.txt"
and idCol="id"
and parser="org.ansj.splitWord.analysis.DicAnalysis"
and inputCol="words";

load parquet.`/tmp/model` as tb;

```

|参数|说明|默认值|
|:---|:---|:---|
|dic.paths|指定自定义词典，多个按逗号分隔||
|idCol|指定记录的唯一标识，方便后续关联回表||
|parser|指定分词器，默认带有NLP功能如命名实体识别等功能,可选的如示例|org.ansj.splitWord.analysis.NlpAnalysis|
|inputCol|等待分词的列|org.ansj.splitWord.analysis.NlpAnalysis|
|filterNatures|指定结果需要过滤出哪些词性|空|
|ignoreNature|是否忽略词性|false|
|deduplicateResult|分词结果是否忽略去掉重复|false|

该功能可以很好的使用在NLP算法领域，毕竟大部分NLP算法工作都需要有一个好的灵活的分词工具。

## 抽词 - TokenExtract


```sql
-- 待处理文本
select "天了噜我是天才" as words ,"1" as id
as newdata;

-- 指定词典，然后TokenExtract 会在/tmp/model生成一个parquet文件，包含id和keywords两个字段。
-- id 是你指定的内容的唯一标号，需要是字符串类型。
-- keywords则是你指定的文本列里抽取出来的在字典中的词汇。

train newdata as TokenExtract.`/tmp/model` where
`dic.paths`="/tmp/abc.txt"
and idCol="id"
and inputCol="words";

load parquet.`/tmp/model` as tb;
```

在词典/tmp/abc.txt 中，里面只有一个词汇 "天了噜"，我们的目标是要包含这个词的内容都能抽取出来。
该功能比较好的适用在关键词匹配上，比正则效率高非常高，而且可以支持成千上万的词汇匹配。

## 字典/表转化为数组字段 - DicOrTableToArray

DicOrTableToArray 是一个很有意思的模型，
很多场景我们需要把表或者字典转化为一个数组类型的字段，那么这个模型可以提供这样的功能。
他会把该数组表示成一个UDF函数，之后通过该UDF函数在任何SQL语句里获取对应的数组。



比如，我们有一个字典/tmp/abc.txt 包含三个字符：

```
a
b
c
```



```sql
 
select "a" 
as a;

-- 这里 a 是任意一张表都行，只是为了符合语法形式。
train a as DicOrTableToArray.`/tmp/model` where 
`dic.paths`="/tmp/abc.txt" 
and `dic.names`="dic1";

register DicOrTableToArray.`/tmp/model` as p;
```

dic.paths 指定字典的路径，多个按','分割
dic.names 指定之巅的名称，多个按','分割

table.paths 指定表，多个按','分割
table.names 指定之巅的名称，多个按','分割

现在，我们可以在其他SQL语句里这么用：

```
-- k 为一个数组字段，结构为 ["a","b","c"]
select p("dic1") as k

-- 比如查看某个数组里面是不是有原来词典里的词汇：

select array_intersect(array("a","帅气"),p("dic1")) as iter

```