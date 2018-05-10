import mlsql_model
import mlsql
from sklearn.ensemble import RandomForestClassifier

clf = RandomForestClassifier(verbose=2)

mlsql.sklearn_configure_params(clf)

X, y = mlsql.sklearn_all_data()

clf.fit(X, y)

X_test, y_test = mlsql.get_validate_data()
if len(X_test) > 0:
    testset_score = clf.score(X_test, y_test)
    print("mlsql_validation_score:%f" % testset_score)

mlsql_model.sk_save_model(clf)
