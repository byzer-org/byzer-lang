import tensorflow as tf
import os
import shutil
import pickle
import mlsql


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


# def keras_save_model(path, model, overwrite=False):
#     newpath = os.path.join(path, "saved_model.h5")
#     if overwrite and os.path.exists():
#         shutil.rmtree(path)
#     model.save(newpath)


def sk_save_model(model):
    isp = mlsql.params()["internalSystemParam"]
    tempModelLocalPath = isp["tempModelLocalPath"] if "tempModelLocalPath" in isp else "/tmp/"
    dir_name = tempModelLocalPath
    if os.path.exists(dir_name):
        shutil.rmtree(dir_name)
    os.makedirs(dir_name)
    with open(os.path.join(dir_name, "model.pickle"), "wb") as f:
        pickle.dump(model, f, protocol=2)
