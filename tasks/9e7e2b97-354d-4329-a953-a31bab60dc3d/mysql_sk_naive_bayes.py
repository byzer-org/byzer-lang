import mlsql_model
import mlsql
from sklearn.naive_bayes import MultinomialNB

rd = mlsql.read_data()
p = mlsql.params()["fitParam"]
batch_size = p["batchSize"] if "batchSize" in p else 1000
model_path = p["modelPath"] if "modelPath" in p else "/tmp/"

clf = MultinomialNB()
for items in rd(max_records=batch_size):
    if len(items) == 0:
        continue
    X = [item["features"].toArray() for item in items]
    y = [item["label"] for item in items]
    print("fit....")
    clf.partial_fit(X, y)

mlsql_model.sk_save_model(model_path, clf)