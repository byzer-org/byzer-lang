

## StreamingPro对机器学习的支持

StreamingPro也对机器学习有一定的支持。训练时，数据的输出不是输出到一个存储器，而是输出成一个模型，其他部分和标准的ETL流程是一样的。
一个典型的的线性回归类算法使用如下：

```
{
  {
    "alg": {
      "desc": "测试",
      "strategy": "spark",
      "algorithm": [],
      "ref": [],
      "compositor": [
        {
          "name": "batch.sources",
          "params": [
            {
              "path": "file:///tmp/sample_linear_regression_data.txt",
              "format": "libsvm",
              "outputTable": "test"
            }
          ]
        },
        {
          "name": "batch.output.alg",
          "params": [
            {
              "algorithm": "lr",
              "path": "file:///tmp/lr-model",
              "inputTableName":"test"
            },
            {
              "maxIter": 1,
              "regParam": 0.3,
              "elasticNetParam": 0.8
            }
          ]
        }
      ],
      "configParams": {
      }
    }
  }
}

```

batch.output.alg 是一个特殊的输出算子。第一个参数包含了算法名称，模型存储路径，输入的数据表。
后面可以配置多组超参数。