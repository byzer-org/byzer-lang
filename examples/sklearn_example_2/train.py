from __future__ import absolute_import
import mlsql
import numpy as np
import os
import json
import sys
import pickle
import scipy.sparse as sp
import importlib
from pyspark.mllib.linalg import Vectors, SparseVector

if sys.version < '3':
    import cPickle as pickle

else:
    import pickle

    xrange = range

unicode = str

def param(key, value):
    if key in mlsql.fit_param:
        res = mlsql.fit_param[key]
    else:
        res = value
    return res

featureCol = param("featureCol", "features")
labelCol = param("labelCol", "label")
moduleName = param("moduleName", "sklearn.svm")
className = param("className", "SVC")
batchSize=1

def load_sparse_data():
    tempDataLocalPath = mlsql.internal_system_param["tempDataLocalPath"]
    # train the model on the new data for a few epochs
    datafiles = [file for file in os.listdir(tempDataLocalPath) if file.endswith(".json")]
    row_n = []
    col_n = []
    data_n = []
    y = []
    feature_size = 0
    row_index = 0
    for file in datafiles:
        with open(tempDataLocalPath + "/" + file) as f:
            for line in f.readlines():
                obj = json.loads(line)
                fc = obj[featureCol]
                if "size" not in fc and "type" not in fc:
                    feature_size = len(fc)
                    dic = [(i, a) for i, a in enumerate(fc)]
                    sv = SparseVector(len(fc), dic)
                elif "size" not in fc and "type" in fc and fc["type"] == 1:
                    values = fc["values"]
                    feature_size = len(values)
                    dic = [(i, a) for i, a in enumerate(values)]
                    sv = SparseVector(len(values), dic)

                else:
                    feature_size = fc["size"]
                    sv = Vectors.sparse(fc["size"], list(zip(fc["indices"], fc["values"])))

                for c in sv.indices:
                    row_n.append(row_index)
                    col_n.append(c)
                    data_n.append(sv.values[list(sv.indices).index(c)])

                if type(obj[labelCol]) is list:
                    y.append(np.array(obj[labelCol]).argmax())
                else:
                    y.append(obj[labelCol])
                row_index += 1
                if row_index % 10000 == 0:
                    print("processing lines: %s, values: %s" % (str(row_index), str(len(row_n))))
                    # sys.stdout.flush()
    print("X matrix : %s %s  row_n:%s col_n:%s classNum:%s" % (
        row_index, feature_size, len(row_n), len(col_n), ",".join([str(i) for i in list(set(y))])))
    sys.stdout.flush()
    return sp.csc_matrix((data_n, (row_n, col_n)), shape=(row_index, feature_size)), y

def create_alg(module_name, class_name):
    module = importlib.import_module(module_name)
    class_ = getattr(module, class_name)
    return class_()

model = create_alg(moduleName, className)

X, y = load_sparse_data()
model.fit(X, y)

tempModelLocalPath = mlsql.internal_system_param["tempModelLocalPath"]

if not os.path.exists(tempModelLocalPath):
    os.makedirs(tempModelLocalPath)

model_file_path = tempModelLocalPath + "/model.pkl"
print("Save model to %s" % model_file_path)
pickle.dump(model, open(model_file_path, "wb"))