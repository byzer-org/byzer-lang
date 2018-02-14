## 算法和工程之殇

算法和工程结合，有两个非常大的痛点：

第一个，算法的着眼点是，用最快速的方式清洗一些数据出来，然后接着建模训练，评估预测效果，之后再重复清洗数据，再试验。因为很多算法工程师都是Python系的，对他们来说，最简单的方式自然是写python程序。一旦确认清洗方式后，这种数据清洗工作，最后研发工程师还要再重新用Spark去实现一遍。那么如果让算法工程师在做数据清洗的时候，直接使用PySpark呢？这样复用程度是不是可以有所提高？实际上是有的。但是算法工程师初期用起来会比较吃力，因为PySpark的学习成本还是有的，而且不小。

第二个，模型的部署。比如我们把一个训练好的tf模型集成到了一个Java应用里，这个很简单，但是还是有难点，难点在哪呢？ 因为模型其实接受的是向量，但是Java应用接受的还是raw数据，需要对raw数据做转化，本质上是把之前Spark实现的清洗流程再重头做一遍。Spark 通过pipeline 解决了这个问题，但是解决的不够好，因为他太慢，没办法用在来一条预测一条的场景里。

我上面提及的两点，是真实存在的，而且极大影响了研发效率。所以数据处理这块，基本整个流程下来，是至少要做三遍的。

1. 算法需要反复清晰数据，直到找到合适的特征集合。
2. 工程要用Spark做一遍工程化
3. 应用服务要用Java/Scala之类的再写一遍针对单条的。

前面提到的是清洗的复用，算法工程师其实喜欢用比如TF,SKlearn等成熟框架，或者独立的高效的单一算法C++实现，比如CRF, LDA等。而工程师则喜欢一些大数据处理套件。 你想让算法工程师天天在Spark上跑，不是一朝一夕的能搞定的。

## 基因论

我一直觉得吴军以前提的基因论真的很有用。比如Spark社区， 整个社区是以工程师为主的，这个就是他的基因。Spark想做做算法，而且很努力，但是总是欠缺那么点意思。

我之前发文吐槽过很多次，包括整个MLlib的API,基本实用价值不大。里面的方法又限制的严，真正算法需要用的方法都访问不到（私有），暴露出来的又没啥用。

我们来看看我实际使用LDA时的尴尬境地：

![image.png](http://upload-images.jianshu.io/upload_images/1063603-4d36dee961e61685.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/620)

还有为了拿里面一个有意义的变量，得这么弄：

![image.png](http://upload-images.jianshu.io/upload_images/1063603-50b3e9585d3c5a00.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/620)

可以看到有多艰难。

我得出的结论是，开发这些算法的人，根本没弄明白算法工程师会怎么用。真正做算法的，也瞧不上spark这套东西。Spark MLlib 应该还是以工程师使用居多。

所以你看tf 之类的一出来，就火了，没spark什么事。

##  尝试解决这些问题

我们现在回顾下问题，再抽象一层就是：

1.  数据处理过程无法在训练和预测时复用
2. 无法很好的衔接算法和工程的框架


###  数据处理过程无法在训练和预测时复用
我把数据处理分成两个部分构成.

1.  聚合，join,字符处理
2. 模型处理

怎么理解这两个部分呢？

首先你需要导出去寻找数据，这些数据散落在各个角落，你需要去各个角落关联，把数据收集起来，接着你会进行各种聚合操作，得到一些统计量，并且对立面的每个字段做些规则上的处理。这个就是对应我前面提到的第一部分 “聚合，join,字符处理”。

第二个部分是，模型处理。为啥数据处理也需要用到模型了？一个简单的示例时，我们需要把字符转化为数字，比如我现在有城市，那么我需要把城市转化为一个递增的int数字，然后把数字转化为ont-hot向量。这个过程我们需要得到一个城市和数字的映射关系，这个关系的建立，本质就是一个模型的建立过程。 泛化的讲，可以认为一切数据处理都是模型，但是如果都抽象成模型，那么模型就是去意义了。我们只对一类很有用，很常见的数据处理抽象成模型。

把数据切分成这两部分有啥好处呢？这就以为这数据处理变得有可复制的可能。但是如果我们都是用Python,用Java去做这些事情，那么还是不能复制，所以我们需要一套更形式化的语言去完成这两部分语言。 这个语言是什么好呢？ 答案是SQL。 SQL天然适合第一部分的工作，第二部分，我们只要适当的扩充`SQL语法`就能高搞定。

这样，在训练时，工程和算法都可以复用一套脚本了。那么预测时候怎么办？我们怎么复用？在预测时候，数据是一条一条来的，所以基本没有前面那个聚合join的过程，只有数据处理的‘模型预测’部分。 比如对一个城市，我们会加载训练时产生的城市到数字的映射模型，然后用模型进行预测，这样就复用了训练时的功能了。

现在，我们解决了数据处理复用的东西，包括训练时工程和算法的复用，也包括训练和预测的复用。

### 无法很好的衔接算法和工程的框架

现在是，工程要用Spark SQL, 算法训练要用TF,怎么办，怎么让他们协作。那就是让他们都看不到底层到底是什么。

首先，数据处理，大家都用经过“扩展的SQL”,接着，算法可以用Tensorflow Python的API定义好网络结构，然后也可以用“扩展的SQL”来完成具体的训练。这样，就统一起来了。


### MLSQL

现在我们看MLSQL是如何优雅的实现这个解决方案的。 我现在有一张表，表里有一个字段叫问题字段，我想用LDA做处理，从而得到问题字段的主题分布，接着我们把主题分布作为向量给一个tensorflow 模型。

```sql

-- 加载一份数据文件，如果是表，则无需load语句
load csv.`/tmp/tfidf.csv`  options header="True" as zhuhl_new_ct;

-- 对text资源进行分词，这里为了简单，我直接用split了
select *,split(question,";") as words from zhuhl_new_ct 
as zhuhl_new_ct;

-- 得到词表
select explode(words) as word_x1 from zhuhl_new_ct) 
as word_table;

-- 训练一个模型，把每个词用一个数字表示
train word_table as StringIndex.`/tmp/zhuhl_si_model` where 
inputCol="word" and handleInvalid="skip";

-- 注册成SQL UDF函数。
register StringIndex.`/tmp/zhuhl_si_model` as zhuhl_si_predict;

-- 把词汇转化为数字，这样问题就被表示成一个数字序列了
select zhuhl_si_predict_array(words) as int_word_seq,label from zhuhl_new_ct
as int_word_seq_table;

-- 训练一个tfidf模型
train int_word_seq_table as TfIdf.`/tmp/zhuhl_tfidf_model` where 
inputCol="int_word_seq"
and numFeatures="4" 
and binary="true";

--注册tfidf模型
register TfIdf.`/tmp/zhuhl_tfidf_model` as zhuhl_tfidf_predict;

--将文本转化为tf/idf向量
select zhuhl_tfidf_predict(int_word_seq) as features,int_word_seq,label from int_word_seq_table
as lda_data;

-- 训练lda模型
train lda_data as LDA.`/tmp/zhuhl_lda_model` where 
 inputCol="features"
 and k="100" 
 and maxIter="10";

--注册lda预测函数
register LDA.`/tmp/zhuhl_lda_model` as zhuhl_lda_predict;

-- 把文本用主题分布表示
select *,zhuhl_lda_predict_doc(features) as features from lda_data
as zhuhl_doclist;

-- 把结果给tensorflow模型
train zhuhl_doclist as TensorFlow.`/tmp/zhuhl_rm_model` 
where pythonDefScript="/tmp/classi" 
and inputCol="features" 
and labelCol="label";

register TensorFlow.`/tmp/zhuhl_rm_model` as zhuhl_rm_predict;
```

这个过程基本不需要研发参与，算法就已经很好的完成了整个流程。我们现在看看如何集成到预测里面去，比如我们希望把最后这些功能都放到一个Java应用里去。

嘴阀很简单，每个数据处理环节以及最后的tf都有一个模型存储地址，Java应用只要加载这些模型就可以，并且，每个模型都有一个SQL开头的实现，比如SQLLDA,里面的话已经提供相应的predict函数（都是针对一条记录的），预测速度会非常快。 写预测程序的人只要把几个模型串联起来就可以了。

如果是异步预测，比如使用SparkStreaming等，则更简单，直接像上面一样注册UDF函数就可以了。
