# Python Environment

Before you can really run your python project, you should create the environment which your project 
depends.

It looks like this:

```sql
set dependencies='''
name: tutorial4
dependencies:
  - python=3.6
  - pip
  - pip:
    - --index-url https://mirrors.aliyun.com/pypi/simple/
    - numpy==1.14.3
    - kafka==1.3.5
    - pyspark==2.3.2
    - pandas==0.22.0
''';

load script.`dependencies` as dependencies;

run command as PythonEnvExt.`/tmp/jack` where condaFile="dependencies" and command="create";
```

If you wanna remove this env, set command to `remove`. Notice that you should make sure all you machines have conda installed 
and the internet connection is ok.

You can also specify  condaYamlFilePath, which is the location of conda.yaml.

When you run python project meets errors like `Could not find Conda executable at conda`, you can add config in 
PythonAlg/PythonParallelExt.

```sql
-- anaconda3 local path
and systemParam.envs='''{"MLFLOW_CONDA_HOME":"/anaconda3"}''';
```