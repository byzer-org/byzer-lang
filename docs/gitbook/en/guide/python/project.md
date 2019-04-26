# Python project standard

MLSQL has low invasion on your python project. You add two description file and then convert this 
project to a MLSQL compatible project. 

Here is the structure of project:

```
examples/sklearn_elasticnet_wine/
├── MLproject
├── batchPredict.py
├── conda.yaml
├── predict.py
├── train.py
```

MLproject describe how to execute the project.
conda.yaml describe how to build python environment.

MLProject contains：

```yaml
name: tutorial

conda_env: conda.yaml

entry_points:
  main:
    train:        
        command: "python train.py"
    batch_predict:                  
        command: "python batchPredict.py"
    api_predict:        
        command: "python predict.py"
```


conda.yaml: 

```
name: tutorial
dependencies:
  - python=3.6
  - pip
  - pip:
    - --index-url https://mirrors.aliyun.com/pypi/simple/
    - numpy==1.14.3
    - kafka==1.3.5
    - pyspark==2.3.2
    - pandas==0.22.0
```

