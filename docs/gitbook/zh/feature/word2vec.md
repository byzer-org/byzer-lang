# Word2Vec

假设我们有如下的数据

```sql
set rawText='''
{"content":"MLSQL是一个好的语言","label":0.0},
{"content":"Spark是一个好的语言","label":1.0}
{"content":"MLSQL语言","label":0.0}
{"content":"MLSQL是一个好的语言","label":0.0}
{"content":"MLSQL是一个好的语言","label":1.0}
{"content":"MLSQL是一个好的语言","label":0.0}
{"content":"MLSQL是一个好的语言","label":0.0}
{"content":"MLSQL是一个好的语言","label":1.0}
{"content":"Spark好的语言","label":0.0}
{"content":"MLSQL是一个好的语言","label":0.0}
''';
```

我们看到，我们把文本分成两类，一个是1,一类是0。现在我们需要判断一个新的文本进来，他的分类是什么。

我们打算通过Word2Vec的方式把文本content转化为一个向量。我们使用ET Word2VecInPlace来完成这个工作。


```sql
load jsonStr.`rawText` as orginal_text_corpus;

train orginal_text_corpus as Word2VecInPlace.`/tmp/word2vec`
where inputCol="content"
and ignoreNature="true"
and resultFeature="merge";

load parquet.`/tmp/word2vec/data` as lwys_corpus_with_featurize;
```

我们会发现content已经被数字化了，里面有type,size,indices,values等四个字段。这是向量稀疏化表示的一种方式。

在上面的示例中，我们只使用了三个参数，完整的参数列表在这里：


|Parameter|Default|Comments|
|:----|:----|:----|
|inputCol|None||
|resultFeature|None|flat:把多个向量拼接成一个向量;merge: 把多个向量按位相加得到一个向量；index: 只输出词序列，每次词用一个数字表示|
|dicPaths|None|用户定义的词典路径，按逗号分给|
|wordvecPaths|None|如果你已经训练了一个word2vec模型，你可以通过该参数指定其路劲。该文本格式为： word + 空格 + 按逗号分隔的数字，类似SVM格式|
|vectorSize|None|词向量长度|
|length|None|文本最大长度|
|stopWordPath|停用词词典||
|split|如果不分词，使用什么进行分隔||
|minCount|词最少出现的次数，低于该次数不会为词生成一个向量||

此处有坑:

>请将ignoreNature设置为true

## 如何在预测时使用

任何ET都具备在"训练时学习到经验"转化为一个函数，从而可以使得你把这个功能部署到流式计算，API服务里去。同时，部分ET还有batch predict功能，
可以让在批处理做更高效的预测。

对于ET TfIdfInPlace 而言，我们要把它转化为一个函数非常容易：

```sql

register TfIdfInPlace.`/tmp/tfidfinplace` as word2vec_convert;

```

通过上面的命令，TfIdfInPlace就会把训练阶段学习到的东西应用起来，现在，任意给定一段文本，都可以使用`tfidf_convert`函数将
内容转化为向量了。

```sql
select word2vec_convert("MLSQL是一个好的语言") as features as output;
```












