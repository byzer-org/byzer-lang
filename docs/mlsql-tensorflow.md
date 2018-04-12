## MLSQL

MLSQL也支持tensorflow的训练和预测。

### 环境配置

因为tensorflow涉及到native lib的问题，所以需要做些配置。

第一步，下载tensorflow的native lib库：

```
TF_TYPE="cpu" # Default processor is CPU. If you want GPU, set to "gpu"
 OS=$(uname -s | tr '[:upper:]' '[:lower:]')
 mkdir -p ./jni
 curl -L \
   "https://storage.googleapis.com/tensorflow/libtensorflow/libtensorflow_jni-${TF_TYPE}-${OS}-x86_64-1.5.0-rc0.tar.gz" |
   tar -xz -C ./jni
```


第二步： 将获得对应的lib，分发到各个yarn节点


第三步：

启动时spark时，worker的java options参数需要带上:

```
-Djava.library.path=[your-path]/jni
```


第四步： 拥有一个可以访问的Kafka,并且允许自动创建topic。

MLSQL对Tensorflow的支持分成两种。第一种为用户自己编写模型代码，第二种是和传统的机器学习算法一样，只要选择算法类型即可。

### 使用现有算法

目前MLSQL支持两个文本分类模型：全连接网络和卷积网络。

卷积网络的代码如下：

```sql
-- 加载训练数据
load libsvm.`/Users/allwefantasy/Softwares/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` 
as data;

-- 对语料进行词向量训练
select label,array_number_to_string(vec_array(features)) as words from data
as new_ct;

train new_ct as word2vec.`/tmp/w2v_model` 
where inputCol="words" and minCount="0";

register word2vec.`/tmp/w2v_model` as w2v_predict;

-- 此时一篇内容会由多个词向量组成，我们会对其展开成一个数组
select label, array_number_concat(w2v_predict_array(words)) as features from new_ct 
as w2v_temp;

-- 把数组转化为向量
select onehot(label,2) as label,vec_dense(features) as features from w2v_temp
as newdata;

-- 获取验证集
select * from newdata  limit 10
as data1;

-- fitParam.0.alg 指定使用CNNClassify算法进行训练
-- wordEmbeddingSize，featureSize ，labelSize是必须配置的。
train newdata as TensorFlow.`/tmp/model` 
where `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and   `kafkaParam.topic`="test"
and   `kafkaParam.group_id`="g_test-1"
and   `kafkaParam.reuse`="false"
and   `fitParam.0.epochs`="1"
and   `fitParam.0.batchSize`="32"
and   `fitParam.0.wordEmbeddingSize`="100"
and   `fitParam.0.featureSize`="69200"
and   `fitParam.0.labelSize`="2"
and   `fitParam.0.alg`="CNNClassify"
and `validateTable`="data1"
and   `systemParam.pythonPath`="python";

-- 注册函数
register  TensorFlow.`/tmp/model`  as tf_predict;

-- 预测
select vec_argmax(tf_predict(features,"features","label",2)) as predict_label,
label from newdata 
```

`fitParam.0.alg`="CNNClassify" 时可用的参数：

| 参数	 | 描述  |备注 |
|:-----------|:------------|:------------|
|fitParam.0.featureSize| 向量长度||
|fitParam.0.wordEmbeddingSize | 词向量长度||
|fitParam.0.labelSize|  分类数目||
|validateTable|  作为测试数据集的表名称||
|fitParam.0.epochs|  对数据集训练多少次,默认1||
|fitParam.0.batchSize|  每个批次多少条数据，默认32||
|fitParam.0.windowGroup|  类似ngram，一次扫描多少个词。默认"5,10,15"||
|fitParam.0.gpuPercent|  使用GPU的比例，默认全部||
|fitParam.0.inputCol|  特征向量列，默认名称为 features||
|fitParam.0.labelCol|  分类列，默认为label||
|fitParam.0.print_interval|  每进行多少批次训练，打印训练和测试准确率，loss值等。默认为1||



全连接的示例代码如下：

```sql
-- tensorflow
load libsvm.`/Users/allwefantasy/Softwares/spark-2.2.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt` as data;

select onehot(label,2) as label,features from data
as newdata;


select * from newdata limit 2 
as data1;


train newdata as TensorFlow.`/tmp/model` 
where `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and   `kafkaParam.topic`="test"
and   `kafkaParam.group_id`="g_test-1"
and   `kafkaParam.reuse`="false"
and   `fitParam.0.layerGroup`="300,100"
and   `fitParam.0.epochs`="1"
and   `fitParam.0.batchSize`="32"
and   `fitParam.0.featureSize`="692"
and   `fitParam.0.labelSize`="2"
and   `fitParam.0.alg`="FCClassify"
and  `validateTable`="data1"
and   `systemParam.pythonPath`="python";

-- 注册函数
register  TensorFlow.`/tmp/model`  as tf_predict;

-- 预测
select vec_argmax(tf_predict(features,"features","label",2)) as predict_label,
label from newdata 
```


`fitParam.0.alg`="FCClassify" 时可用的参数：

| 参数	 | 描述  |备注 |
|:-----------|:------------|:------------|
|fitParam.0.featureSize| 向量长度||
|fitParam.0.labelSize|  分类数目||
|validateTable|  作为测试数据集的表名称||
|fitParam.0.epochs|  对数据集训练多少次,默认1||
|fitParam.0.batchSize|  每个批次多少条数据，默认32||
|fitParam.0.layerGroup|  每个隐层的单元数，默认只有一个隐层，为300||
|fitParam.0.gpuPercent|  使用GPU的比例，默认全部||
|fitParam.0.inputCol|  特征向量列，默认名称为 features||
|fitParam.0.labelCol|  分类列，默认为label||
|fitParam.0.print_interval|  每进行多少批次训练，打印训练和测试准确率，loss值等。默认为1||

### 自定义算法


方便测试，我们直接使用Spark自带的一个libsvm文件。我省去了路径。

```sql
load libsvm.`[location]/sample_libsvm_data.txt` as data;

```

libsvm格式的data表默认有features和label两个字段。

接着开始训练：

```python
-- 训练tf模型。
train data as TensorFlow.`/tmp/model` 
where pythonDescPath="/example.py"
and `kafkaParam.bootstrap.servers`="127.0.0.1:9092"
and `kafkaParam.topic`="test-9_1517059642136"
and `kafkaParam.group_id`="g_test-1"
and `kafkaParam.reuse`="true"
and `fitParam.0.epochs`="10"
and  `fitParam.0.max_records`="10"
and `systemParam.pythonPath`="python";
;
```

参数解释：

"/tmp/model" 表示模型的存放位置。
"pythonDescPath" tensorflow训练脚本的位置。

"kafka." 相关的配置参数。每次训练都会新建一个主题，比如你把topic脚a,那么MLSQL会自动给你生成一个叫a_1517059642136(时间戳)的表。
这样可能会生成很多topic,为了避免这个问题，你可以通过配置kafkaParam.reuse，表示我就复用某个topic。

mlsql支持给定多组配置参数方便进行并行训练。fitParam.0 表示第一组，fitParam.1 表示第二组，以此类推。数字后面的参数为具体的参数。

"systemParam." 目前只可以配置python的路径。


在example.py文件中，你可以导入如下几个支持文件：

```
import mlsql_model
import mlsql

```

获取数据：

```python
data = mlsql.read_data()

for items in data(max_records=2):
    X = [item["features"].toArray() for item in items]
    Y = [trans(item["label"]) for item in items]
    if len(X) > 0:
        _, gs = sess.run([train_step, global_step],
                         feed_dict={input_x: X, input_y: Y})
```


训练好的模型我想保存：

```python
p = mlsql.params()
mlsql_model.save_model(p["fitParam"]["modelPath"], sess, input_x, input_y, True)
```

可以通过mlsql.params拿到所有的配置参数（where条件后面的参数）。这里我们想拿 fitParam.modelPath,如果没有配置，默认会使用Tensorflow.`path`
这个路径。

一个完整的实例代码：

```python
import tensorflow as tf
import mlsql_model
import mlsql
import sys

rd = mlsql.read_data()

tf.reset_default_graph
config = tf.ConfigProto()
config.gpu_options.per_process_gpu_memory_fraction = 0.3
sess = tf.Session(config=config)
input_x = tf.placeholder(tf.float32, [None, 692], name="input_x")
input_y = tf.placeholder(tf.float32, [None, 2], name="input_y")
global_step = tf.Variable(0, name='global_step', trainable=False)


def fc_layer(input, size_in, size_out, active="relu", name="fc"):
    with tf.name_scope(name):
        w = tf.Variable(tf.truncated_normal([size_in, size_out], stddev=0.1), name="W_" + name)
        b = tf.Variable(tf.constant(0.1, shape=[size_out], name="B_" + name))
        if active == "sigmoid":
            act = tf.nn.sigmoid(tf.matmul(input, w) + b)
        elif active is None:
            act = tf.matmul(input, w) + b
        else:
            act = tf.nn.relu(tf.matmul(input, w) + b)
        tf.summary.histogram("W_" + name + "_weights", w)
        tf.summary.histogram("B_" + name + "_biases", b)
        tf.summary.histogram(name + "_activations", act)
        return act


fc1 = fc_layer(input_x, 692, 300, "relu", "fc1")
_logits = fc_layer(fc1, 300, 2, "relu", "fc2")
tf.identity(_logits, name="result")

with tf.name_scope("xent"):
    xent = tf.reduce_mean(
        tf.nn.softmax_cross_entropy_with_logits(logits=_logits, labels=input_y), name="xent"
    )
    tf.summary.scalar("xent", xent)

with tf.name_scope("train"):
    train_step = tf.train.AdamOptimizer(0.001).minimize(xent, global_step=global_step)

with tf.name_scope("accuracy"):
    correct_prediction = tf.equal(tf.argmax(_logits, 1), tf.argmax(input_y, 1))
    accurate = tf.reduce_mean(tf.cast(correct_prediction, tf.float32), name="accuracy")
    tf.summary.scalar("accuracy", accurate)

summ = tf.summary.merge_all()

sess.run(tf.global_variables_initializer())


def trans(i):
    if i == 0:
        return [0, 1]
    if i == 1:
        return [1, 0]


for items in rd(max_records=2):
    X = [item["features"].toArray() for item in items]
    Y = [trans(item["label"]) for item in items]
    if len(X) > 0:
        _, gs = sess.run([train_step, global_step],
                         feed_dict={input_x: X, input_y: Y})
        [train_accuracy, s, loss] = sess.run([accurate, summ, xent],
                                             feed_dict={input_x: X, input_y: Y})
        print('train_accuracy %g, loss: %g, global step: %d' % (
            train_accuracy,
            loss,
            gs))
    sys.stdout.flush()
p = mlsql.params()
mlsql_model.save_model(p["fitParam"]["modelPath"], sess, input_x, input_y, True)
sess.close()

```

### 注册UDF函数

```sql
-- 加载tf模型，并且提供预测函数
register TensorFlow.`/tmp/model`  as tf_predict;
```

### 使用预测函数

```sql
-- 使用模型做预测，得到最后的分类
select vec_argmax(tf_predict(features,"input_x","result",2)) as predict_label,
label from data as result;
```



