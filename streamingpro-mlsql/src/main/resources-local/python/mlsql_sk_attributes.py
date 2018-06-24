from sklearn import svm
import os
import json
import numpy as np
import pickle
X = [[0, 0], [1, 1]]
y = [0, 1]
clf = svm.SVC()
clf.fit(X, y)
with open('/tmp/svm.pickle', 'wb') as fw:
    pickle.dump(clf, fw)
def save_attributes(model_file, attributes_file):
    with open(model_file, 'rb') as fr:
        new_svm = pickle.load(fr)
        attribute_dict_numpy = {k: repr(v.tolist()).replace("\n", "").replace(" ", "") for (k, v) in clf.__dict__.items() if (type(v) == np.ndarray)}
        attribute_dict_normal = dict2 = {k: repr(v) for (k, v) in clf.__dict__.items() if (type(v) != np.ndarray)}
        attribute_dict = dict(attribute_dict_numpy, **attribute_dict_normal)
        attribute_json = json.dumps(attribute_dict)
        with open(attributes_file, 'w') as f:
            f.write(attribute_json)
params_file = os.path.join(os.getcwd(), "python_temp.pickle")

model_path = "set the value as /tmp/svm.pickle when running test unit"
with open(params_file, 'rb', 1 << 20) as f:
    params = pickle.load(f)
    if "modelPath" in params:
        model_path = params["modelPath"]
save_attributes(model_path, "/tmp/attributes.json")