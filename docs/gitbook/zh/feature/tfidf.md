# TFIDF

【文档更新日志：2020-04-14】

> Note: 本文档适用于MLSQL Engine 1.2.0 及以上版本。  
> 对应的Spark版本可支持2.3.2/2.4.3
>

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

我们打算通过TFIDF的方式把文本content转化为一个向量。我们使用ET TfIdfInPlace来完成这个工作。


```sql
load jsonStr.`rawText` as orginal_text_corpus;


train orginal_text_corpus as TfIdfInPlace.`/tmp/tfidfinplace`
where inputCol="content"
and ignoreNature="true"
and nGrams="2,3"
as tfTable;

select * from tfTable as output;
```

经过上面的处理，我们看看结果:

![](http://docs.mlsql.tech/upload_images/WX20200414-154955.png)

我们会发现content已经被数字化了，里面有type,size,indices,values等四个字段。这是向量稀疏化表示的一种方式。

在上面的示例中，我们只使用了三个参数，完整的参数列表在这里：


|Parameter|Default|Comments|
|:----|:----|:----|
|inputCol|需要处理的字段名称||
|dicPaths|None|用户自定义词典，注意在Yarn上是HDFS路径|
|stopWordPath|用户自定义停用词词典，注意在Yarn上是HDFS路径||
|priorityDicPath|优先级高的词典，注意在Yarn上是HDFS路径||
|priority||对于优先级高的词典里，我们应该提权多少倍|
|nGrams|None|词组合，比如设置为2,3则表示分别以两个词，三个词进行组合得到新词|
|split|可选，如果你不想用分词，可以自己定义切分字符||
|ignoreNature|分词后是否在每个词上都带上词性。请设置为true||

此处有坑:

>请将ignoreNature设置为true

## 如何在预测时使用

任何ET都具备在"训练时学习到经验"转化为一个函数，从而可以使得你把这个功能部署到流式计算，API服务里去。同时，部分ET还有batch predict功能，
可以让在批处理做更高效的预测。

对于ET TfIdfInPlace 而言，我们要把它转化为一个函数非常容易：

```sql

register TfIdfInPlace.`/tmp/tfidfinplace` as tfidf_convert;

```

通过上面的命令，TfIdfInPlace就会把训练阶段学习到的东西应用起来，现在，任意给定一段文本，都可以使用`tfidf_convert`函数将
内容转化为向量了。

```sql
select tfidf_convert("MLSQL是一个好的语言") as features as output;
```












