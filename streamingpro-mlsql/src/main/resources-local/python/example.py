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
mlsql_model.save_model(p["internalSystemParam"]["tempModelLocalPath"], sess, input_x, input_y, True)
sess.close()
