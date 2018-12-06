import mlsql_model
import mlsql
from sklearn.naive_bayes import MultinomialNB

clf = MultinomialNB()

mlsql.sklearn_configure_params(clf)


def train(X, y, label_size):
    clf.partial_fit(X, y, classes=range(label_size))


mlsql.sklearn_batch_data(train)

X_test, y_test = mlsql.get_validate_data()
if len(X_test) > 0:
    testset_score = clf.score(X_test, y_test)
    print("mlsql_validation_score:%f" % testset_score)

mlsql_model.sk_save_model(clf)
