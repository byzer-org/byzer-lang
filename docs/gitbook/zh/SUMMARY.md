<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Summary

## TABLE OF CONTENTS

* 概要
    * [MSLQL简介](getting_started/README.md)
        * [MSLQL-Engine](getting_started/mlsql-engine.md)
        * [MLSQL-Cluster](getting_started/mlsql-cluster.md)
        * [MLSQL-Console](getting_started/mlsql-console.md)

* 用户指南
    * [编译&运行&部署](installation/README.md)
        * [自助编译](installation/compile.md)
        * [使用Docker](installation/docker.md)   
        * [如何运行](installation/run.md)
        * [启动参数说明](installation/startup-configuration.md)
    
    * [数据源](datasource/README.md)      
        * [JDBC](datasource/jdbc.md)
        * [ElasticSearch](datasource/es.md)
        * [Solr](datasource/solr.md)
        * [HBase](datasource/hbase.md)
        * [MongoDB](datasource/mongodb.md)
        * [Parquet/Json/Text/Xml/Csv](datasource/file.md)
        * [jsonStr/script/mlsqlAPI/mlsqlConf](datasource/mlsql_source.md)        
        * [其他](datasource/other.md)
    
    * [变量设置](variable/README.md)
        * [Conf](variable/conf.md)
        * [Shell](variable/shell.md)
        * [Sql](variable/sql.md)        
    
    * [数据处理](process/README.md)
        * [Select 语法](process/select.md)
        * [Run 语法](process/run.md)
        * [Train 语法](process/train.md)
        * [Save 语法](process/save.md)   
        * [内置Estimator/Transformer](process/estimator_transformer/README.md)
           * [直接操作MySQL](process/estimator_transformer/JDBC.md)
           * [计算复杂的父子关系](process/estimator_transformer/TreeBuildExt.md)
           * [改变表的分区数](process/estimator_transformer/RepartitionExt.md)
           * [如何发送邮件](process/estimator_transformer/SendMessage.md) 
           * [如何缓存表](process/estimator_transformer/CacheExt.md)   
    
    * [任务管理](jobs/README.md)
        * [查看正在任务列表]()
        * [取消正在运行任务]()
    
    * [多租户]()
        * [多租户相关参数]()
        * [租户主目录设置]()        
    
    * [创建UDF/UDAF]()
        * [Python UDF]()
        * [Python UDAF]()
        * [Scala UDF]()
        * [Scala UDAF]()
    
    * [系统UDF函数列表]()
        * [http请求]()
        * [向量操作]()
    
    * [Python项目支持]()
        * [Python项目规范]()
        * [分布式运行Python项目]()
        * [单实例运行Python项目]()
        * [如何附带资源文件]()  
    
    * [项目化脚本]()
        * [脚本如何互相引用]()
        
    * [流式计算]()
        * [MLSQL流式计算概念简介]()
        * [数据源]()
           * [Kafka]()
           * [Mock]()
        * [如何将JSON/CSV转化为表]()
        * [数据写入]()           
    
    * [特征工程组件]()
          
      * [文本向量化操作-TfIdf]()
      * [文本向量化操作-Word2Vec]()
      * [ScalerInPlace]()
      * [ConfusionMatrix]()
      * [FeatureExtract]()
      * [NormalizeInPlace]()
      * [ModelExplainInPlace]()
      * [Discretizer]()
        * [bucketizer]()
        * [quantile]()
      * [OpenCVImage]()
      * [VecMapInPlace]()
      * [JavaImage]()
      * [TokenExtract / TokenAnalysis]()
      * [RateSampler]()
      * [RowMatrix]()
      * [CommunityBasedSimilarityInPlace]()
      * [Word2ArrayInPlace]()
      * [WaterMarkInPlace]()      
      * [MapValues]()
                       
    * [Python算法]()
        * [集成SKlearn示例]()
        * [集成TensorFlow示例]()
        * [TensorFlow Cluster支持]()
    
    * [MLSQL内置算法]()
        * [NaiveBayes]()
        * [ALS]()
        * [RandomForest]()
        * [GBTRegressor]()
        * [LDA]()
        * [KMeans]()
        * [FPGrowth]()
        * [GBTs]()
        * [LSVM]()
        * [PageRank]()
        * [LogisticRegressor]()
        * [XGBoost]()
    
    * [深度学习]()
        * [加载图片数据]()
        * [Cifar10示例]()
        
    * [部署算法API服务]()
        * [设计和原理]()
        * [案例剖析]()        
        
    * [爬虫]()
        * [爬虫示例]()
        * [基于MLSQL爬虫系统的设计]()
    
    * [保障数据安全]()
        * [MLSQL统一授权体系]()
        * [如何开发自定义授权规则]()
    
    * [管理多个MLSQL实例]()
        * [MLSQL-Cluster设计和原理]()
        * [MLSQL-Cluster部署]()
        
* 开发者指南
    * [如何参与开发]()
    * [开发者列表]() 
* MLSQL实战            
         
    
           
           


    
        