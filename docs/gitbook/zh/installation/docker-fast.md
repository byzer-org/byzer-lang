# 使用docker快速体验

后续章节会有的很多功能点都提供了大量的范例代码，其中绝大部分是可以直接运行的，使用Docker基本可以零成本学习和使用这些功能。
所以如果只是为了学习目的，可以使用Docker镜像来辅助。

本章节，我们会教会大家就启动一个MLSQL-Engine来跑一些实例代码。

首先，打开网页：

```
https://hub.docker.com/r/techmlsql/mlsql/tags
```

选择合适的版本，我们这里选择标签为 `spark_2.4-${VERSION}`的版本。这个名字表示，这是基于spark 2.4(你可以体验最新的Spark内核).

现在让我们run起来：

```shell
docker run -p 9003:9003 techmlsql/mlsql:spark_2.4-${VERSION} 
```

Ok, that's all.

现在你可以访问 http://127.0.0.1:9003页面了。如果你想要一个更好的界面，请参看前面章节使用MLSQL-Console。

接着把如下代码年贴进编辑器：

```sql

set rawText='''
{"content":"MLSQL提供了机器学习和大数据解决方案","label":0.0},
{"content":"Spark是一个好的语言","label":1.0}
{"content":"MLSQL语言","label":0.0}
{"content":"MLSQL是一个好的语言","label":0.0}
{"content":"","label":1.0}
{"content":"MLSQL是一个好的语言","label":0.0}
{"content":"MLSQL是一个分布式引擎","label":0.0}
{"content":"MLSQL是一个好的语言","label":0.0}
{"content":"Spark好的语言","label":1.0}
{"content":"MLSQL是一个好的语言","label":0.0}
''';

load jsonStr.`rawText` as orginal_text_corpus;

-- 做特征处理
train orginal_text_corpus as TfIdfInPlace.`/tmp/tfidfinplace`
where inputCol="content"
and ignoreNature="true"
and nGrams="2,3";

load parquet.`/tmp/tfidfinplace/data` as lwys_corpus_with_featurize;

--使用随机森林训练
train lwys_corpus_with_featurize as RandomForest.`/tmp/model` where
keepVersion="false" 
and `fitParam.0.featuresCol`="content"
and `fitParam.0.labelCol`="label"
and `fitParam.0.maxDepth`="10"
;

-- 注册个个模型为函数
register TfIdfInPlace.`/tmp/tfidfinplace` as tfidf_predict;
register RandomForest.`/tmp/model` as rf_predict;
```

恭喜，你训练了第一个文本分类算法。现在让我们让我们试试这个模型的效果吧,重新输入如下语句：

```sql
--新数据预测
select  vec_argmax(rf_predict(tfidf_predict("MLSQL 真的很库哦"))) as predict_label as output;
```

结果输出如下：

```
predict_label
0
```