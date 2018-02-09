import mlsql_model
import mlsql
from sklearn.svm import SVC

rd = mlsql.read_data()
p = mlsql.params()["fitParam"]
isp = mlsql.params()["internalSystemParam"]
batch_size = int(p["batchSize"]) if "batchSize" in p else 1000
tempModelLocalPath = isp["tempModelLocalPath"] if "tempModelLocalPath" in isp else "/tmp/"

clf = SVC()

mlsql.sklearn_configure_params(clf)

X, y = mlsql.sklearn_all_data()

clf.fit(X, y)

mlsql_model.sk_save_model(tempModelLocalPath, clf)
