import os
import tensorflow as tf
import json
from tensorflow.contrib.learn.python.learn.datasets.mnist import read_data_sets

import mlsql


def param(key, value):
    if key in mlsql.fit_param:
        res = mlsql.fit_param[key]
    else:
        res = value
    return res


jobName = param("jobName", "worker")
taskIndex = int(param("taskIndex", "0"))
clusterSpec = json.loads(mlsql.internal_system_param["clusterSpec"])
checkpoint_dir = mlsql.internal_system_param["checkpointDir"]

if not os.path.exists(checkpoint_dir):
    os.makedirs(checkpoint_dir)


print(mlsql.internal_system_param["clusterSpec"])
print(jobName)
print(taskIndex)


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
        # workers perform the operation
        # ps_strategy = tf.contrib.training.GreedyLoadBalancingStrategy(FLAGS.num_ps)

        # Note: tf.train.replica_device_setter automatically place the paramters (Variables)
        # on the ps hosts (default placement strategy:  round-robin over all ps hosts, and also
        # place multi copies of operations to each worker host
        with tf.device(tf.train.replica_device_setter(worker_device="/job:worker/task:%d" % (taskIndex),
                                                      cluster=cluster)):
            # load mnist dataset
            mnist = read_data_sets("./dataset", one_hot=True)

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
                    if step % 100 == 0:
                        print("Train step %d, loss: %f" % (step, ls))

run()
