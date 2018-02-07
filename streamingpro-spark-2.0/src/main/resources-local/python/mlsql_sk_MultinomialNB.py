import mlsql_model
import mlsql
from sklearn.naive_bayes import MultinomialNB

rd = mlsql.read_data()
p = mlsql.params()["fitParam"]
batch_size = int(p["batchSize"]) if "batchSize" in p else 1000
model_path = p["modelPath"] if "modelPath" in p else "/tmp/"
label_size = int(p["labelSize"]) if "labelSize" in p else "/tmp/"
clf = MultinomialNB()
for items in rd(max_records=batch_size):
    if len(items) == 0:
        continue
    X = [item["features"].toArray() for item in items]
    y = [item["label"] for item in items]
    clf.partial_fit(X, y, classes=range(label_size))

mlsql_model.sk_save_model(model_path, clf)
