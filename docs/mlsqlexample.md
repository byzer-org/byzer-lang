## 使用演示

首先我们需要启动StreamingPro作为一个sql server ，[如何启动](https://github.com/allwefantasy/streamingpro/blob/master/README.md#启动一个sqlserver服务)
现在你可以通过rest接口提交SQL脚本给该服务了。

首先，我们加载一个csv文件：

```sql
load csv.`/tmp/test.csv` options header="True" as ct;
```
 csv内容如下：

```
body
a b c
a d m
j d c
a b c
b b c
```
这个csv文件被映射为表名ct。只有一个字段body。现在我们需要对body字段进行切分，这个也可以通过sql来完成：

```
select split(body," ") as words from ct as new_ct;
```

新表叫new_ct,现在，可以开始训练了,把new_ct喂给word2vec即可：

```
train new_ct as word2vec.`/tmp/w2v_model` where inputCol="words";
```

word2vec表示算法名， `/tmp/w2v_model ` 则表示把训练好的模型放在哪。where 后面是模型参数。

最后，我们注册一个sql函数：

```
register word2vec.`/tmp/w2v_model` as w2v_predict;
```

其中w2v_predict是自定义函数名。这样，我们在sql里就可以用这个函数了。我们来用一把：

```
select words[0] as w, w2v_predict(words[0]) as v from new_ct as result;
```
给一个词，就可以拿到这个词的向量了。

我们把它保存成json格式作为结果：

```
save result as csv.`/tmp/result`;
```

结果是这样的：![WX20180113-131009@2x.png](http://upload-images.jianshu.io/upload_images/1063603-63c169001e02e894.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/620)

最后完整的脚本如下：

```
load csv.`/tmp/test.csv` options header="True" as ct;
select split(body," ") as words from ct as new_ct;
train new_ct as word2vec.`/tmp/w2v_model` where inputCol="words";
register word2vec.`/tmp/w2v_model` as w2v_predict;
select words[0] as w, w2v_predict(words[0]) as v from new_ct as result;
save overwrite result as json.`/tmp/result`;
```

大家可以用postman测试：
![WX20180113-131211@2x.png](http://upload-images.jianshu.io/upload_images/1063603-1960fd63dd9f2fc3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/620)


##支持算法（不断更新）

* NaiveBayes
* RandomForest
* GBTRegressor
* LDA
* KMeans
* FPGrowth
* GBTs
* LSVM
* LogisticRegressor

## 总结
通过将机器学习算法SQL脚本化，很好的衔接了数据处理和训练，预测。同时服务化很好的解决了环境依赖问题。当然终究是没法取代写代码，但是简单的任务就可以用简单的方式解决了。