# 特征工程组件

MLSQL提供了非常多的特征工程Estimator/Transformer（本章节我们都会简称ET）。
做机器学习一大痛点就是训练阶段的特征工程代码无法复用在API 预测阶段。 MLSQL的 ET很好的解决了这个问题。

MLSQL内置这些ET有如下特点：

1. 训练阶段可用，保证吞吐量
2. 预测阶段使用，保证性能，一般毫秒级

在下面章节中，我们会看到具体如何使用ET来解决这个痛点。

## 系统需求

启动MLSQL时，请使用--jars带上 [ansj_seg-5.1.6.jar](https://github.com/allwefantasy/streamingpro/releases/download/v1.1.0/ansj_seg-5.1.6.jar),[nlp-lang-1.7.8.jar](https://github.com/allwefantasy/streamingpro/releases/download/v1.1.0/nlp-lang-1.7.8.jar).
因为在很多示例中，我们需要用到分词相关的功能。







