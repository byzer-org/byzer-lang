# 欢迎进入MLSQL的世界-MLSQL解决了什么问题


## 前言

先给MLSQL做个定义：

1.  MLSQL是首先是一门语言，SQL的超集。 这意味着他的门槛足够低，无论分析师，研发，算法，运营，产品经理都可以用。
2. MLSQL 同时也是一个跨平台的支持私有部署和云部署的分布式计算引擎。这意味着你可以充分利用MLSQL的算力，完成大部分算法和大数据相关的工作。

MLSQL践行了用一个语言，一个平台去完成大数据和机器学习相关的工作。

## 数据中台的概念（让我们炒个概念）

在谈MLSQL解决了什么问题之前，我们先提一个“数据中台”的概念。什么是数据中台呢？数据中台至少应该具备如下四个特点：

1. 数据中台整合一切内外数据。中台底层的数据组织不再局限于集中式的数据仓库，数据仓库只是中台的一个数据源。中台表现层面的数据形态是联邦制的，大家可以参考我这篇文章：[数据部门起步阶段需要建立数仓么？](https://www.jianshu.com/p/eb3beca560b0)，这里面对数据的整合做了比较详细的描述。

2. 数据中台整合一切内外服务，这种服务形态可以是UDF函数，可以是ET(MLSQL术语，Estimator/Transformer缩写)。在数据中台中，除了传统数据部提供的服务以外，还包括公司内外一切API服务，你可以利用这些API服务帮助你进行数据的探索，加工。这也得益于后端的微服务化，以及类似k8s调度的兴起，让后端抗压能力越来越水平。印证了我前面说的，数据中台是前端，后端，数据发展的共同产物。

3. 数据中台是可编程的。任何托拉拽最终的结局都是无法满足新的需求，需要根据需求开发，所以在中台提供了一个可以涵盖批处理，流式计算，机器学习，提供API开发等统一一致的语言是必要的。同样的，这个语言还要能很好的和其他语言，比如Scala/Java, Python等进行交互和集成。同时，这个编程语言要足够简单，才能面向产品，运营，商务等非技术体系的同仁。

4. 数据中台不仅仅与人交互，还可以和机器交互。这是什么意思的呢？ 数据中台是产品，运营，商务，分析师，后端，前端，算法，数据研发们的日常工作台，同时他们也可以把自己的写的脚本（工作）在数据中台里直接对外提供成API,简单的form表单，从而实现和其他的服务交互。

简而言之，数据中台应该是分析师，算法，研发，产品，运营甚至老板日常工作的集中式的控制台。MLSQL就可以做成这么一件事，为什么呢？

正如前言所述，MLSQL是一门语言，一个分布式引擎，并且支持各种数据源，所以他天然适合做数据中台。


![image.png](https://upload-images.jianshu.io/upload_images/1063603-8576e74450573a53.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

## 大数据研发同学看这里的痛点

1. 很多情况大数据平台和算法平台是割裂的，这就意味着人员和工作流程，还有研发方式，语言都是割裂。
2. 在大数据平台里面，批处理，流式，API服务等等都是割裂的。我们依赖各种语言，比如Scala/Java/Go/Python等等，每个人写出的代码各有千秋，分析师，研发，数仓各自看不懂对方的东西。
3. 起点低，都快进入了2019年了，很多同学们还在用一些比较原始的技术和理念，比如还在大量使用类似yarn调度的方式去做批任务，流式也还停留在JStorm,Spark Streaming等技术上。哪怕是没有多少历史包袱的公司也是。
4. 我们会有大量的项目需要维护，而在我看来，一个中小型大数据部门，2-3个核心项目仓库是最理想的。代码维护是昂贵的。

MLSQL首先是弥合了大数据平台和算法平台的割裂，这是因为MLSQL对算法有着非常友好的支持。我们看一个比较典型的示例：

```sql
-- load data
load parquet.`${rawDataPath}` as orginal_text_corpus;

-- select only columns we care
select feature,label from orginal_text_corpus as orginal_text_corpus;

-- feature enginere moduel
train zhuml_orginal_text_corpus  as TfIdfInPlace.`${tfidfFeaturePath}` 
where inputCol="content" 
and `dic.paths`="/data/dict_word.txt" 
and stopWordPath="/data/stop_words"
and nGrams="2";

-- load data
load parquet.`${tfidfFeaturePath}/data` as tfidfdata;

--  algorithm module
train zhuml_corpus_featurize_training as PythonAlg.`${modelPath}` 
where pythonScriptPath="${projectPath}"
-- distribute data
and  enableDataLocal="true"
and  dataLocalFormat="json"
-- sklearn params
and `fitParam.0.moduleName`="sklearn.svm"
...
and `fitParam.1.moduleName`="sklearn.naive_bayes"
....
and `fitParam.1.labelSize`="2"

-- convert model to udf
register  TfIdfInPlace.`${tfidfFeaturePath}` as tfidf_transform;
register  PythonAlg.`${modelPath}` as classify_predict;

-- predict
select classify_predict(tfidf_transform(feature)) from orginal_text_corpus as output;
```

这段脚本完成了数据加载，处理，tfidf化，并且使用两个算法进行训练,注册模型为函数，最后调用函数进行预测，一气呵成。大家可以看这个[PPT](https://www.slidestalk.com/s/MLSQLApowerfulDSLforBIgDataandAI11606),了解MLSQL如何进行批，流，算法，爬虫相关的工作。

第二个问题，如何统一流批，API, 在上面的PPT里大家可以看到，所有工作都使用MLSQL语言完成，除此之外你可以整合各类API服务，用MLSQL完成诸如爬虫，发邮件，生成下载链接等等功能。

第三个问题，MLSQL底层集合了譬如Spark,Tensorflow/Sklearn等各种主流技术以及大数据相关的思想，所以其实并不需要你关注太多。

## 算法的同学看这里的痛点

我们假设大部分算法的代码都是基于Python的：

1.  项目难以重现，可阅读性和环境要求导致能把另外一个同事写的python项目运行起来不得不靠运气
2.  和大数据平台衔接并不容易，需要让研发重新做工程实现，导致落地周期变长。
3.  训练时数据预处理/特征化无法在预测时复用
4. 集成到流式，批处理和提供API服务都不是一件容易的事情
5. 代码/算法复用级别有限，依赖于算法自身的经验以及自身的工具箱，团队难以共享。
6.  其他团队很难接入算法的工作

这些问题我们慢慢看。首先是项目难以重现，环境问题。MLSQL 的PythonALg模块可以集成任何Python算法项目。我们通过借鉴MLFlow的一些思想可以很好的解决Python环境依赖问题，并且比MLFlow具有更少的侵入性。用户只要在自己的项目里添加一个包依赖文件就可以很好的解决。

第二个和大数据平台衔接，我[PPT](https://www.slidestalk.com/s/MLSQLApowerfulDSLforBIgDataandAI11606)里还有个so sad 系列：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-48124b2ed69bfaa5.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

基本算法工程师搞了个算法，很可能需要两周才能上线，你说怎么才能迭代变快。两周上线不可怕，可怕的是每个项目都是如此。那么MLSQL怎么去解决呢？ 正如前面提到的， MLSQL 的PythonALg模块可以集成任何Python算法项目，算法工程师只要把项目丢进MLSQL就可以转化为一个算法模块，然后使用MLSQL语言调用。所以天然就是衔接的。如果用户不使用Python,那更好，MLSQL自己也集成了深度学习和传统机器学习相关的库，你可以用现成的。

第三个问题，”训练时数据预处理/特征化无法在预测时复用“，我们知道，在训练时，都是批量数据，而在预测时，都是一条一条的，本身模式就都是不一样的。所以传统的模式是很难复用的，在MLSQL里，所谓数据处理无非就是 SQL+UDF Script+Estimator/Transformer, 前面两个复用其实没啥问题，Estimator/Transformer 在训练时，接受的是批量的数据，并且将学习到的东西保存起来，之后在预测时，会将学习到的东西转化函数，然后使用函数对单条数据进行预测。大家看这个图就明白了。

![image.png](https://upload-images.jianshu.io/upload_images/1063603-7f00fff07e9fa879.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

第四个问题，”集成到流式，批处理和提供API服务都不是一件容易的事情“，因为任何算法或者处理逻辑都可以被MLSQL自动转化为一个UDF函数，所以可以无缝的衔接进流式和批处理里。那么如何提供API服务呢？MLSQL核心理念如下：

![image.png](https://upload-images.jianshu.io/upload_images/1063603-8c7b735d2377e045.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

我们可以把训练阶段的模型，udf, python/scala code都转化为函数，然后串联函数就可以了。无需任何开发，就可以部署出一个端到端的API服务。大家可以看到，一个标准的API服务本质就是一个select语句。

5，6两个问题，因为大家都用同一个语言，也就没有难么困难的交流了。研发可以为算法提供更多的预处理Estimator/Transformer,算法也可以提供更多的算法Estimator/Transformer。


## 分析师同学的痛点看这里

分析师大部分都是写SQL, hive script其实shell + SQL, 这无形又需要分析师懂shell了， shell是一门神奇的语言，主要是他不正规，没有标准委员会去约束。这是第一个痛点。

第二个痛点是啥呢， SQL难以复用。 复用体现在几个层面，第一，同一条SQL里有多个相同的case when语句，我得手写很多次。第二个是，SQL表的复用，SQL执行完一般就是一张表，如果我想复用这张表，那我就得写hive表，写hive表很痛苦，耗时并且占用存储，成本高。我能不能构建类似视图的东西呢？比如我需要A表，A 其实就是一条SQL，我需要的时候include这种A就好了。

第三个痛点是，我啥事都得靠你研发，比如处理一个东西依赖的UDF函数，都得等你研发搞。那我能不能自己用Python写一个UDF,不需要编译，不需要上线，还能复用呢？

这些问题如何解决呢？MLSQL的解决方式在这篇文章里 [如何按程序员思维写分析师脚本](https://www.jianshu.com/p/239415ef4385)


## 所有同学的痛点

所有同学的痛点，其实就是协作痛点。不同同学讲的语言不一样，你用Java,我用SQL,我用Scala,我用C++。 MLSQL怎么解决这个痛点呢？同一个语言，同一个平台。

## MLSQL会不会不够灵活，限制我们的能力？

如果是简单的SQL其实肯定无法满足算法和工程的要求呢，而MLSQL是SQL的超集,这意味着 MLSQL具备高度扩展能力，这包括：

1. Estimator/Transformer ,前面我们看到train语法里的TfIdfInPlace，PythonAlg，这些模块你可以用现成的，研发也可以扩展，然后--jars带入后就可以使用。
2. MLSQL提供了在脚本中写python/scala UDF/UDAF的功能，这就意味着你可以通过代码无需编译和部署就能扩展MLSQL的功能。
3. MLSQL 的PythonALg模块可以集成任何Python算法项目。我们通过借鉴MLFlow的一些思想可以很好的解决Python环境依赖问题，并且比MLFlow具有更少的侵入性。


## MLSQL 到底想干嘛
前面的数据中台概念里，我们提到了全公司数据视图，得益于我们底层依赖的Spark,我们基本上可以load任何类型的数据源，所以你可以实现不挪动数据的情况，就可以把数仓里的数据，业务的数据库，甚至execel放在一起进行join.

MLSQL就是想成为前面我们描述的一个数据中台，整合大数据和机器学习还有分析的所有流程。他的终极目标也很简单，让你的工作更轻松。

MLSQL 官网地址是:      http://www.mlsql.tech
MLSQL的github地址： https://github.com/allwefantasy/streamingpro
MLSQL博客地址：https://www.jianshu.com/c/759bc22b9e15

## 客户有话说

> 刚开始推的时候，我被喷的不行，好多SQL跑不出来，写的各种复杂，然后跟我说数据库都能跑出来，为啥大数据平台跑不出来。。。然后我给他们调优，都跑出来了，最后觉得MLSQL挺好用的。但是现在要用的人太多，简直奔溃。

点评：产品在推广之初，总是会受到各种质疑。能顶住压力推广，真的很重要，后面获益也会很多。

> MLSQL是一个可玩性很高的平台。
> 确实，感觉做的很好，自由度很高。

点评： MLSQL毕竟是一个语言，而且可以很容易做成坨坨拽拽的产品，自动生成MLSQL脚本。

> 从MLSQL中可以看到世界级影响力产品的潜质

点评： 客户这么夸真的扛不住。。。。。



## Q/A:

> Q1: 大中台还要能支持上层业务快速的灵活定制，上线，MLSQL能做到么？

A: MLSQL  是一个脚本语言，无需编译和部署，调试完毕即可上线，所以天生适合上层业务的灵活性。我们举个如何用MLSQL实现爬虫功能的例子来说明如何快速的满足业务需求。假设我们需要一个快速构建一个爬虫服务，但是MLSQL自带的浏览器渲染功能满足不了诉求，这个时候我们可以开发一个浏览器渲染的服务，其API输入是URL,输出是经过渲染后的html。其他所有功能全部用MLSQL来完成。实现上会是这个样子的：

```
set chrome_render_api="http:....."
load crawller.`http://www.csdn.net/blog/ai` where xpath=..... as url_table;
select http("${chrome_render_api}",map(......)) as html,url from url_table as htmls;
.......更多处理
---存储进数据库
save result as jdbc.`db.table`....
```

开发完毕后，如果有业务需求变更，我直接更改脚本，然后发布，重新设置定时任务，基本上是0成本的，而且大家都看得懂。