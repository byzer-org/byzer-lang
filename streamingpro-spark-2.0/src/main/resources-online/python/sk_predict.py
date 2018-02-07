from pyspark.ml.linalg import VectorUDT, Vectors
import pickle
import python_fun


def predict(index, s):
    items = [i for i in s]
    feature = VectorUDT().deserialize(pickle.loads(items[0]))
    model = pickle.loads(pickle.loads(items[1])[0])
    y = model.predict([feature.toArray()])
    return [VectorUDT().serialize(Vectors.dense(y))]


python_fun.udf(predict)
