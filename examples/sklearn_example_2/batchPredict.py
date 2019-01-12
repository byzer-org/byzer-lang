import mlsql
import pickle
import json
import os
from pyspark.ml.linalg import VectorUDT, Vectors
from pyspark.mllib.linalg import SparseVector

# get information from mlsql
isp = mlsql.params()["internalSystemParam"]
tempDataLocalPath = isp["tempDataLocalPath"]
tempModelLocalPath = isp["tempModelLocalPath"]
tempOutputLocalPath = isp["tempOutputLocalPath"]

model = pickle.load(open(tempModelLocalPath + "/model.pkl", "rb"))

def param(key, value):
    if key in mlsql.params()["fitParams"]:
        res = mlsql.params()["fitParams"][key]
    else:
        res = value
    return res


featureCol = param("featureCol", "features")

def parse(line):
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
    return sv

with open(tempOutputLocalPath, "w") as o:
    with open(tempDataLocalPath) as f:
        for line in f.readlines():
            features = parse(line)
            y = model.predict([features])
            o.write(json.dumps({"predict": y.tolist()}) + "\n")
