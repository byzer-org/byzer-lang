# TensorFlow 集群模式

MLSQL 初步支持TF集群。主要是包装TF原生的Cluster模式。

首先按集群模式写好脚本py_train.mlsql：

```python
set py_train='''
import json
import os
import sys

import tensorflow as tf
from tensorflow.contrib.learn.python.learn.datasets.mnist import read_data_sets

from pyjava.api.mlsql import PythonProjectContext

context = PythonProjectContext()
context.read_params_once()

roleSpec = json.loads(context.conf["roleSpec"])
jobName = roleSpec["jobName"]
taskIndex = int(roleSpec["taskIndex"])
clusterSpec = json.loads(context.conf["clusterSpec"])


print("------jobName: %s  taskIndex:%s-----" % (jobName, str(taskIndex)))
print(clusterSpec)


def model(images):
    """Define a simple mnist classifier"""
    net = tf.layers.dense(images, 500, activation=tf.nn.relu)
    net = tf.layers.dense(net, 500, activation=tf.nn.relu)
    net = tf.layers.dense(net, 10, activation=None)
    return net


def run():
    # create the cluster configured by `ps_hosts' and 'worker_hosts'
    cluster = tf.train.ClusterSpec(clusterSpec)

    # create a server for local task
    server = tf.train.Server(cluster, job_name=jobName,
                             task_index=taskIndex)

    if jobName == "ps":
        server.join()  # ps hosts only join
    elif jobName == "worker":
        checkpoint_dir = context.output_model_dir()

        if not os.path.exists(checkpoint_dir):
            os.makedirs(checkpoint_dir)
        with open(context.input_data_dir()) as f:
            for item in f.readlines():
                print(item)
        # workers perform the operation
        # ps_strategy = tf.contrib.training.GreedyLoadBalancingStrategy(FLAGS.num_ps)

        # Note: tf.train.replica_device_setter automatically place the paramters (Variables)
        # on the ps hosts (default placement strategy:  round-robin over all ps hosts, and also
        # place multi copies of operations to each worker host
        with tf.device(tf.train.replica_device_setter(worker_device="/job:worker/task:%d" % (taskIndex),
                                                      cluster=cluster)):
            # load mnist dataset
            print("image dir:%s" % context.input_data_dir())
            mnist = read_data_sets("./dataset", one_hot=True, source_url="http://docs.mlsql.tech/upload_images/")

            # the model
            images = tf.placeholder(tf.float32, [None, 784])
            labels = tf.placeholder(tf.int32, [None, 10])

            logits = model(images)
            loss = tf.reduce_mean(tf.nn.softmax_cross_entropy_with_logits(logits=logits, labels=labels))

            # The StopAtStepHook handles stopping after running given steps.
            hooks = [tf.train.StopAtStepHook(last_step=2000)]

            global_step = tf.train.get_or_create_global_step()
            optimizer = tf.train.AdamOptimizer(learning_rate=1e-04)

            if True:
                # asynchronous training
                # use tf.train.SyncReplicasOptimizer wrap optimizer
                # ref: https://www.tensorflow.org/api_docs/python/tf/train/SyncReplicasOptimizer
                optimizer = tf.train.SyncReplicasOptimizer(optimizer, replicas_to_aggregate=2,
                                                           total_num_replicas=2)
                # create the hook which handles initialization and queues
                hooks.append(optimizer.make_session_run_hook((taskIndex == 0)))

            train_op = optimizer.minimize(loss, global_step=global_step,
                                          aggregation_method=tf.AggregationMethod.ADD_N)

            # The MonitoredTrainingSession takes care of session initialization,
            # restoring from a checkpoint, saving to a checkpoint, and closing when done
            # or an error occurs.
            with tf.train.MonitoredTrainingSession(master=server.target,
                                                   is_chief=(taskIndex == 0),
                                                   checkpoint_dir=checkpoint_dir,
                                                   hooks=hooks) as mon_sess:

                while not mon_sess.should_stop():
                    # mon_sess.run handles AbortedError in case of preempted PS.
                    img_batch, label_batch = mnist.train.next_batch(32)
                    _, ls, step = mon_sess.run([train_op, loss, global_step],
                                               feed_dict={images: img_batch, labels: label_batch})
                    print("Train step %d, loss: %f" % (step, ls))
                    sys.stdout.flush()


run()


''';

load script.`py_train` as py_train;
```

代码比较简单，是标准的TF Cluster模式的写法，区分了ps/worker.
这里，为了和MLSQL进行交互，我们需要引入pyjava相关的类，获得一个context，
具体做法如下：


```python
context = PythonProjectContext()
context.read_params_once()
```

然后调用一次（只允许一次）获得所有的参数。现在你可以拿到MLSQL给你准备好的数据目录，默认是json格式，
我这里只是简单的将结果打印出来。

```python
with open(context.input_data_dir()) as f:
            for item in f.readlines():
                print(item)
```

以及模型目录，模型目录是你可以将你训练好的模型要放到`context.output_model_dir()`中，之后
MLSQL会自动同步到HDFS上.

定义conda文件，如果你事先创建了环境，可以直接设置为空py_env.mlsql：

```sql
set py_env='''
''';
load script.`py_env` as py_env;
```

现在，我们可以运行了：

```sql
select 1 as a as data;

include demo.`tf.py_train.mlsql`;
include store1.`alg.python.text_classify.py_env.mlsql`;

train data as DTF.`/tmp/tf/model`
where scripts="py_train"
and entryPoint="py_train"
and condaFile="py_env"
and  keepVersion="true"
and fitParam.0.fileFormat="json" -- 还可以是parquet
and `fitParam.0.psNum`="1"
and PYTHON_ENV="streamingpro-spark-2.4.x";
```

这会让MLSQL启动一个worker， 一个ps进行训练。 worker数量取决于数据的分区数。ps的数量则取决于`fitParam.0.psNum` 参数的配置。
PYTHON_ENV 允许你指定环境。

点击运行，系统会将脚本所有信息实时输出

```
INFO DriverLogServer: [owner] [allwefantasy@gmail.com] [groupId] [3dc017e2-0a13-4eff-8534-628c381a5171] Train step 1995, loss: 0.127047
19/08/27 15:08:40  INFO DriverLogServer: [owner] [allwefantasy@gmail.com] [groupId] [3dc017e2-0a13-4eff-8534-628c381a5171] Train step 1996, loss: 0.161283
19/08/27 15:08:40  INFO DriverLogServer: [owner] [allwefantasy@gmail.com] [groupId] [3dc017e2-0a13-4eff-8534-628c381a5171] Train step 1996, loss: 0.107826
19/08/27 15:08:40  INFO DriverLogServer: [owner] [allwefantasy@gmail.com] [groupId] [3dc017e2-0a13-4eff-8534-628c381a5171] Train step 1997, loss: 0.176538
19/08/27 15:08:40  INFO DriverLogServer: [owner] [allwefantasy@gmail.com] [groupId] [3dc017e2-0a13-4eff-8534-628c381a5171] Train step 1997, loss: 0.053615
19/08/27 15:08:40  INFO DriverLogServer: [owner] [allwefantasy@gmail.com] [groupId] [3dc017e2-0a13-4eff-8534-628c381a5171] Train step 1998, loss: 0.025230
19/08/27 15:08:40  INFO DriverLogServer: [owner] [allwefantasy@gmail.com] [groupId] [3dc017e2-0a13-4eff-8534-628c381a5171] Train step 1998, loss: 0.100496
19/08/27 15:08:40  INFO DriverLogServer: [owner] [allwefantasy@gmail.com] [groupId] [3dc017e2-0a13-4eff-8534-628c381a5171] Train step 1999, loss: 0.101606
19/08/27 15:08:40  INFO DriverLogServer: [owner] [allwefantasy@gmail.com] [groupId] [3dc017e2-0a13-4eff-8534-628c381a5171] Train step 1999, loss: 0.436057
19/08/27 15:10:40  INFO DriverLogServer: [owner] [allwefantasy@gmail.com] [groupId] [3dc017e2-0a13-4eff-8534-628c381a5171] 2019-08-27 15:10:40.986701: I tensorflow/core/distributed_runtime/worker.cc:199] Cancellation requested for RunGraph.
19/08/27 15:10:58  INFO DriverLogServer: [owner] [allwefantasy@gmail.com] [groupId] [3dc017e2-0a13-4eff-8534-628c381a5171] bash: line 1: 55593 Terminated: 15
```

最后的返回结果：

```
host            port    jobName taskIndex  isPs    done      success
192.168.204.142	2222	worker	0	       false   true	     true
192.168.204.142	2221	ps	    0	       true	   true	     true
```

我们可以看到所有节点的情况。有任何一个节点success不为true,则表示训练失败。

Tensorflow的Cluster我们还在持续完善。目前调度还不够完善，可能多个节点会跑在一台服务器上，对于使用了GPU的机器就显得不够友好了。

