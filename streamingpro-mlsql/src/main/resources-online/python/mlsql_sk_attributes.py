import os
import json
import numpy as np
import pickle
def save_attributes(model_file, attributes_file):
    with open(model_file, 'rb') as fr:
        model = pickle.load(fr)
        attribute_dict_numpy = {k: repr(v.tolist()).replace("\n", "").replace(" ", "") for (k, v) in model.__dict__.items() if (type(v) == np.ndarray)}
        attribute_dict_normal = {k: repr(v) for (k, v) in model.__dict__.items() if (type(v) != np.ndarray)}
        attribute_dict = dict(attribute_dict_numpy, **attribute_dict_normal)
        if (hasattr(model, "feature_importances_")):
            func = getattr(model, "feature_importances_")
            attribute_dict["feature_importances_"] = repr(func).replace("\n", "").replace(" ", "")
        attribute_json = json.dumps(attribute_dict)
        with open(attributes_file, 'w') as f:
            f.write(attribute_json)
params_file = os.path.join(os.getcwd(), "python_temp.pickle")

model_path = ""
tempModelLocalPath = ""
with open(params_file, 'rb', 1 << 20) as f:
    params = pickle.load(f)
    if "modelPath" in params:
        model_path = params["modelPath"]
    if "internalSystemParam" in params:
        isp = params["internalSystemParam"]
        if "tempModelLocalPath" not in isp:
            raise Exception("tempModelLocalPath is not configured")
        tempModelLocalPath = isp["tempModelLocalPath"]


save_attributes(model_path, tempModelLocalPath + "/attributes.json")