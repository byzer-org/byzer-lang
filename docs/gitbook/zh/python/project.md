# Python项目规范

MLSQL对Python项目具有极低的侵入性，你只要额外增加两个问题就可以让一个标准的python程序变成MLSQL可识别的Python项目。
我们有一个如下的示例项目：

```
examples/sklearn_elasticnet_wine/
├── MLproject
├── batchPredict.py
├── conda.yaml
├── predict.py
├── train.py
```

里面有两个文件，MLproject和 conda.yaml 是需要额外提供的。

MLProject文件内容：

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
MLProject 指定了项目名称以及conda.yaml描述文件。
同时指定了该项目的入口。

一般我们会指定train/batch_predict/api_predict 其中的一项或者多项。当你有一个机器学习项目，并且需要部署成API服务的时候，才会需要三个入口都指定。
如果仅仅是使用Python做数据处理，只需要配置train入口即可。

所以MLProject其实是一个描述文件，指明在什么场景跑什么脚本。

第二个是conda.yaml文件：

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

描述每个人物需要依赖的环境。