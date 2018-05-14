## MLSQL-SKLearn

### 前提条件：

1. 拥有一个可以访问的Kafka(1.0 版本以上),并且允许自动创建topic。
2. 安装了SKLearn相关库


### 使用MLSQL训练，加载，使用SKlearn:

```sql
-- 加载数据
load libsvm.`/Users/allwefantasy/Softwares/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;

--训练SKLearn贝叶斯模型
train data as SKLearn.`/tmp/model`  
where `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and `kafkaParam.topic`="test"
and `kafkaParam.group_id`="g_test-1"
and  `fitParam.0.batchSize`="1000"
and  `fitParam.0.labelSize`="2"
and  `fitParam.0.alg`="MultinomialNB"
and `systemParam.pythonPath`="python";
and `systemParam.pythonVer`="2.7";
;

-- 注册模型
register SKLearn.`/tmp/model` as nb_predict;

-- 预测
select vec_array(nb_predict(features))  as k from data
```

其中，因为MultinomialNB采用了SKlearn里的partial_fit,所以可以通过设置fitParam.0.batchSize 表示每批次给算法数据量。

fitParam.0.labelSize 告诉分类数目
systemParam.pythonPath 和 systemParam.pythonVer 分别设置executor节点python的路径和版本。kafkaParam 则是配置一个kafka实例。


### 加载非MLSQL训练得到的SKearn模型

如果你希望注册一个不是通过MLSQL训练的模型，那么可以使用Pickle保存模型：

```sql
from sklearn import datasets
        iris = datasets.load_iris()
        from sklearn.naive_bayes import GaussianNB
        gnb = GaussianNB()
        gnb.fit(iris.data, iris.target)
        with open(os.path.join("./", "sklearn_model_iris.pickle"), "wb") as f:
            pickle.dump(gnb, f, protocol=2)
```

然后就可以注册了：

```sql
register SKLearn.`/tmp/models/sklearn_model_iris.pickle` as predict
options nonMLSQLModel="true"
and pythonPath="python"
and pythonVersion="3.6"
```

nonMLSQLModel告诉系统，模型是用户自己通过pickle生成的，另外我们还需要告诉系统对应的python路径和版本,以便系统能够采用合适的
python环境去加载模型。


### 目前支持的算法有

1. GradientBoostingClassifier
2. RandomForestClassifier
3. SVC
4. MultinomialNB
