import tensorflow as tf
import mlsql_model
import mlsql
import sys
import mlsql_tf

rd = mlsql.read_data()
p = mlsql.params()

fitParams = p["fitParam"]

tf.reset_default_graph
config = tf.ConfigProto()

gpuPercent = float(mlsql.get_param(fitParams, "gpuPercent", -1))
featureSize = int(mlsql.get_param(fitParams, "featureSize", -1))
label_size = int(mlsql.get_param(fitParams, "labelSize", -1))
layer_group = [int(i) for i in mlsql.get_param(fitParams, "layerGroup", "300").split(",")]

batch_size = int(mlsql.get_param(fitParams, "batchSize", 32))
epochs = int(mlsql.get_param(fitParams, "epochs", 1))
print_interval = int(mlsql.get_param(fitParams, "printInterval", 1))

input_col = mlsql.get_param(fitParams, "inputCol", "features")
label_col = mlsql.get_param(fitParams, "labelCol", "label")
tempModelLocalPath = p["internalSystemParam"]["tempModelLocalPath"]

if featureSize < 0 or label_size < 0:
    raise RuntimeError("featureSize or labelSize is required")

if gpuPercent > 0:
    config.gpu_options.per_process_gpu_memory_fraction = gpuPercent

sess = tf.Session(config=config)
input_x = tf.placeholder(tf.float32, [None, featureSize], name=input_col)
input_y = tf.placeholder(tf.float32, [None, label_size], name="input_y")
global_step = tf.Variable(0, name='global_step', trainable=False)

input_size = featureSize
output_size = label_size
input_temp = input_x
output_temp = None

for layer in layer_group:
    output_size = layer
    output_temp = mlsql_tf.fc_layer(input_temp, input_size, output_size, "relu", "fc{}".format(layer))
    input_temp = output_temp
    input_size = output_size

_logits = mlsql_tf.fc_layer(output_temp, input_size, label_size, "relu", "fc{}".format(label_size))
tf.identity(_logits, name=label_col)

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

TEST_X, TEST_Y = mlsql.get_validate_data()
TEST_Y = [item.toArray() for item in TEST_Y]

for ep in range(epochs):
    for items in rd(max_records=batch_size):
        X = [item[input_col].toArray() for item in items]
        Y = [item[label_col].toArray() for item in items]
        if len(X) == 0:
            print("bad news , this round no message fetched")
        if len(X) > 0:
            _, gs = sess.run([train_step, global_step],
                             feed_dict={input_x: X, input_y: Y})
            if gs % print_interval == 0:
                [train_accuracy, s, loss] = sess.run([accurate, summ, xent],
                                                     feed_dict={input_x: X, input_y: Y})
                [test_accuracy, test_s, test_lost] = sess.run([accurate, summ, xent],
                                                              feed_dict={input_x: TEST_X, input_y: TEST_Y})
                print('train_accuracy %g,test_accuracy %g, loss: %g, test_lost: %g,global step: %d, ep:%d' % (
                    train_accuracy,
                    test_accuracy,
                    loss,
                    test_lost,
                    gs, ep))
                sys.stdout.flush()

    mlsql_model.save_model(tempModelLocalPath, sess, input_x, _logits, True)
    sess.close()
