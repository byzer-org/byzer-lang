# Python项目

MLSQL除了支持前面提到的脚本处理，还支持复杂的Python项目。为了让MLSQL能够识别出你的项目，需要有两个额外的描述文件：

1. MLproject
2. conda.yaml 

一个比较典型的目录结构如下：

```
examples/sklearn_elasticnet_wine/
├── MLproject
├── batchPredict.py
├── conda.yaml
├── predict.py
├── train.py
```

## MLProject

一个典型的MLProject文件是这样的：

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

## conda.yaml

我们其实在前面已经见过，这个是一个标准的conda环境描述文件。

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


## 如何使用

通常而言，conda.yaml文件的环境需要事先通过使用PythonEnvExt来进行创建，否则可能存在并发环境创建而导致失败。
对于一个Python的项目使用，我们有两种使用场景：

1. 分布式运行这个Python项目，主要用于数据处理。
2. 将数据全部放到一个节点上，然后使用该Python项目进行处理。通常用于模型构建。

我们在下面的文章中，会分成两个部分来进行阐述。