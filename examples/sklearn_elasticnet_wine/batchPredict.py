import mlsql
import pickle
import json
import os
from pyspark.ml.linalg import VectorUDT, Vectors

# get information from mlsql
isp = mlsql.params()["internalSystemParam"]
tempDataLocalPath = isp["tempDataLocalPath"]
tempModelLocalPath = isp["tempModelLocalPath"]
tempOutputLocalPath = isp["tempOutputLocalPath"]

print("tempModelLocalPath:%s" % (tempModelLocalPath))
model = pickle.load(open(tempModelLocalPath + "/model.pkl", "rb"))

print("tempDataLocalPath:%s" % (tempDataLocalPath))
with open(tempOutputLocalPath, "w") as o:
    with open(tempDataLocalPath) as f:
        for line in f.readlines():
            obj = json.loads(line)
            features = []
            for attribute, value in obj.items():
                if attribute != "quality":
                    features.append(value)
            y = model.predict([features])
            o.write(json.dumps({"predict": y.tolist()}) + "\n")
