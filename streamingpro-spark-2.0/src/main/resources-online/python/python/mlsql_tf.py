import tensorflow as tf


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


def conv_poo_layer(input, size_in, size_out, filter_width, filter_height, include_pool=False, name="conv"):
    with tf.name_scope(name):
        w = tf.Variable(tf.truncated_normal([filter_height, filter_width, size_in, size_out], stddev=0.1), name="W")
        b = tf.Variable(tf.constant(0.1, shape=[size_out], name="B"))
        conv = tf.nn.conv2d(input, w, strides=[1, 1, 1, 1], padding="VALID")

        act = tf.nn.relu(conv + b)

        tf.summary.histogram("weights", w)
        tf.summary.histogram("biases", b)
        tf.summary.histogram("activations", act)

        if include_pool:
            return tf.nn.max_pool(act, ksize=[1, filter_height, 1, 1], strides=[1, 1, 1, 1], padding="VALID")
        else:
            return act
