## Intro to NLP Classification Job with MLSQL

### Before We Start the Tutorial

We will build a small text classification job during this tutorial. You might be tempted to skip it because you’re not building games — but give it a chance.
The techniques you’ll learn in the tutorial are fundamental to building any Machine Learning job with MLSQL,
and mastering it will give you a deep understanding of MLSQL.

This tutorial is designed for people who prefer to learn by doing.
If you prefer learning concepts from the ground up, check out our step-by-step guide.
You might find this tutorial and the guide complementary to each other.


The tutorial is divided into several sections:

[Setup for the Tutorial]() will give you a starting point to follow the tutorial.
[How to load data]() will give you the power of collecting data from any data source.
[Feature engineer]() will teach you the fundamentals of feature engineer in MLSQL： e.g. vectorize, onehot label,split data to train / test set.
[How to use RandomForest]() will teach you how to apply embedded algorithm in MLSQL, by this tutorial, you also know how to save multi-version model,how to use model.


### Setup for the Tutorial

Please refer [Installation](https://github.com/allwefantasy/streamingpro/blob/master/docs/docv2/getting_started/installation.md) to setup MLSQL.
Notice that you should make sure ansj_seg-5.1.6.jar and nlp-lang-1.7.8.jar are configured with --jars.

Suppose for now ,you have already finish the setup of MLSQL. Now we should prepare the training data.

Download tar.gz file from  http://download.labs.sogou.com/dl/sogoulabdown/SogouCS/news_sohusite_xml.smarty.tar.gz.

```
tar xzvf news_sohusite_xml.smarty.tar.gz
```

### How to load data

Once you have setup the MLSQL, you should visit http://127.0.0.1:9003. The first