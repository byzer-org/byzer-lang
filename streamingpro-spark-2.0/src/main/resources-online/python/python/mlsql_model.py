import tensorflow as tf
import os
import shutil


def save_model(path, session, input_tensor, output_tensor, overwrite=False):
    if overwrite and os.path.exists(os.path.join(path, "saved_model.pb")):
        shutil.rmtree(path)
    signature = tf.saved_model.signature_def_utils.build_signature_def(
        inputs={'input': tf.saved_model.utils.build_tensor_info(input_tensor)},
        outputs={'output': tf.saved_model.utils.build_tensor_info(output_tensor)},
    )
    builder = tf.saved_model.builder.SavedModelBuilder(path)
    builder.add_meta_graph_and_variables(session,
                                         [tf.saved_model.tag_constants.SERVING],
                                         signature_def_map={
                                             tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY: signature})
    builder.save()
