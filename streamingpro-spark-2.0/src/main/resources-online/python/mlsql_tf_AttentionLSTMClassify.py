# import tensorflow as tf
# import mlsql_model
# import mlsql
# import sys
# import mlsql_tf
#
# rd = mlsql.read_data()
# p = mlsql.params()
#
# fitParams = p["fitParam"]
#
# tf.reset_default_graph
# config = tf.ConfigProto()
#
# gpuPercent = float(mlsql.get_param(fitParams, "gpuPercent", -1))
# featureSize = int(mlsql.get_param(fitParams, "featureSize", -1))
# wordEmbeddingSize = int(mlsql.get_param(fitParams, "wordEmbeddingSize", -1))
# sequenceLen = featureSize / wordEmbeddingSize
#
# label_size = int(mlsql.get_param(fitParams, "labelSize", -1))
# layer_group = [int(i) for i in mlsql.get_param(fitParams, "layerGroup", "300").split(",")]
#
# print_interval = int(mlsql.get_param(fitParams, "printInterval", 1))
#
# window_group = [int(i) for i in mlsql.get_param(fitParams, "windowGroup", "5,10,15").split(",")]
#
# batch_size = int(mlsql.get_param(fitParams, "batchSize", 32))
# epochs = int(mlsql.get_param(fitParams, "epochs", 1))
#
# input_col = mlsql.get_param(fitParams, "inputCol", "features")
# label_col = mlsql.get_param(fitParams, "labelCol", "label")
# tempModelLocalPath = p["internalSystemParam"]["tempModelLocalPath"]
#
# if featureSize < 0 or label_size < 0 or wordEmbeddingSize < 0:
#     raise RuntimeError("featureSize or labelSize or wordEmbeddingSize is required")
#
# if gpuPercent > 0:
#     config.gpu_options.per_process_gpu_memory_fraction = gpuPercent
#
# INITIAL_LEARNING_RATE = 0.001
# INITIAL_KEEP_PROB = 0.9
#
# sess = tf.Session(config=config)
#
# input_x = tf.placeholder(tf.float32, [None, featureSize], name=input_col)
# _input_x = tf.reshape(input_x, [-1, sequenceLen, wordEmbeddingSize])
# input_y = tf.placeholder(tf.float32, [None, label_size], name="input_y")
# global_step = tf.Variable(0, name='global_step', trainable=False)
#
# lstm_cell = tf.nn.rnn_cell.LSTMCell(num_units=64, state_is_tuple=True)
# cell = tf.contrib.rnn.AttentionCellWrapper(lstm_cell)
# encoder_outputs, encoder_final_state = tf.nn.bidirectional_dynamic_rnn(cell=cell,dtype=tf.float32,inputs=_input_x,sequence_length=)
#
#
# fc1 = mlsql_tf.fc_layer(final_flattened, int(final_flattened.shape[1]), 1024, "relu", "fc1")
# _logits = mlsql_tf.fc_layer(fc1, 1024, label_size, None, "fc2")
# tf.identity(_logits, name=label_col)
# with tf.name_scope("xent"):
#     xent = tf.reduce_mean(
#         tf.nn.softmax_cross_entropy_with_logits(logits=_logits, labels=input_y), name="xent"
#     )
#     tf.summary.scalar("xent", xent)
#
# with tf.name_scope("train"):
#     learning_rate = tf.train.exponential_decay(INITIAL_LEARNING_RATE, global_step,
#                                                1200, 0.8, staircase=True)
#     train_step = tf.train.AdamOptimizer(learning_rate).minimize(xent, global_step=global_step)
#
# with tf.name_scope("accuracy"):
#     correct_prediction = tf.equal(tf.argmax(_logits, 1), tf.argmax(input_y, 1))
#     accurate = tf.reduce_mean(tf.cast(correct_prediction, tf.float32), name="accuracy")
#     tf.summary.scalar("accuracy", accurate)
#
# summ = tf.summary.merge_all()
#
# sess.run(tf.global_variables_initializer())
# # writer = tf.summary.FileWriter(TENSOR_BORAD_DIR)
# # writer.add_graph(sess.graph)
# #
# # writer0 = tf.summary.FileWriter(TENSOR_BORAD_DIR + "/0")
# # writer0.add_graph(sess.graph)
#
# saver = tf.train.Saver()
#
# test_items = mlsql.get_validate_data()
# TEST_X = [item[input_col].toArray() for item in test_items]
# TEST_Y = [item[label_col].toArray() for item in test_items]
#
# for ep in range(epochs):
#     for items in rd(max_records=batch_size):
#         X = [item[input_col].toArray() for item in items]
#         Y = [item[label_col].toArray() for item in items]
#         _, gs = sess.run([train_step, global_step],
#                          feed_dict={input_x: X, input_y: Y})
#         if gs % print_interval == 0:
#             [train_accuracy, s, loss] = sess.run([accurate, summ, xent],
#                                                  feed_dict={input_x: X, input_y: Y})
#             [test_accuracy, test_s, test_lost] = sess.run([accurate, summ, xent],
#                                                           feed_dict={input_x: TEST_X, input_y: TEST_Y})
#             print('train_accuracy %g,test_accuracy %g, loss: %g,test_lost: %g, global step: %d, ep:%d' % (
#                 train_accuracy,
#                 test_accuracy,
#                 loss,
#                 test_lost,
#                 gs, ep))
#             sys.stdout.flush()
#
# mlsql_model.save_model(tempModelLocalPath, sess, input_x, _logits, True)
# sess.close()
