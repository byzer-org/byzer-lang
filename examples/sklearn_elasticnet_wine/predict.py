from pyspark.ml.linalg import VectorUDT, Vectors
import pickle
import os
import python_fun


def predict(index, s):
    items = [i for i in s]
    feature = VectorUDT().deserialize(pickle.loads(items[0]))
    print(pickle.loads(items[1])[0])
    model = pickle.load(open(pickle.loads(items[1])[0] + "/model.pkl", "rb"))
    y = model.predict([feature.toArray()])
    return [VectorUDT().serialize(Vectors.dense(y))]


python_fun.udf(predict)
