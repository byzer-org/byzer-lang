import mlsql_model
import mlsql
from sklearn.ensemble import GradientBoostingClassifier

clf = GradientBoostingClassifier()

mlsql.sklearn_configure_params(clf)

X, y = mlsql.sklearn_all_data()

clf.fit(X, y)

mlsql_model.sk_save_model(clf)
