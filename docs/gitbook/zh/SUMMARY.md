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
    * [MSLQL简介](getting_started/READEME.md)
        * [MLSQL-Engine](getting_started/mlsql-engine.md)
        * [MLSQL-Cluster](getting_started/mlsql-cluster.md)
        * [MLSQL-Console](getting_started/mlsql-console.md)

* 用户指南
    * [编译&运行&部署](installation/README.md)
        * [自助编译](installation/compile.md)
        * [一键体验MLSQL套件](installation/docker.md)   
        * [如何运行](installation/run.md)
        * [启动参数说明](installation/startup-configuration.md)
        * [使用docker快速体验MLSQL-Engine](installation/docker-fast.md)
        * [MLSQL IDE开发环境配置](installation/ide.md)
        
    * [数据源](datasource/README.md)      
        * [JDBC](datasource/jdbc.md)
        * [ElasticSearch](datasource/es.md)
        * [Solr](datasource/solr.md)
        * [HBase](datasource/hbase.md)
        * [MongoDB](datasource/mongodb.md)
        * [Parquet/Json/Text/Xml/Csv](datasource/file.md)
        * [jsonStr/script/mlsqlAPI/mlsqlConf](datasource/mlsql_source.md)        
        * [其他](datasource/other.md)
        * [运行时添加数据源依赖](datasource/dynamically_add.md)
    
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
    
    * [任务/资源管理](jobs/README.md)
        * [查看正在任务列表](jobs/list_jobs.md)
        * [取消正在运行任务](jobs/cancel_job.md)
        * [动态添加/删除资源](jobs/dynamic_resource.md)
    
    * [多租户](multi_tenement/README.md)
        * [多租户相关参数](multi_tenement/conf.md)
        * [租户主目录设置](multi_tenement/home.md)
        * [租户主目录查看与管理](multi_tenement/home_fs.md)        
    
    * [创建UDF/UDAF](udf/README.md)
        * [Python UDF](udf/python_udf.md)       
        * [Scala UDF](udf/scala_udf.md)
        * [Scala UDAF](udf/scala_udaf.md)
        * [Java UDF](udf/java_udf.md)
    
    * [系统UDF函数列表](system_udf/README.md)
        * [http请求](system_udf/http.md)
        * [常见函数](system_udf/vec.md)
    
    * [Python项目支持](python/README.md)
        * [Python项目规范](python/project.md)
        * [Python环境管理](python/python-env.md)        
        * [分布式运行Python项目](python/distribute-python.md)
        * [单实例运行Python项目](python/python.md)
        * [如何附带资源文件](python/resource.md)  
    
    * [项目化脚本](include/README.md)        
        * [如何简化一条SQL语句](include/sql.md)        
        * [如何统一管理scala/python udf脚本](include/include_script.md)        
        * [如何统一管理中间表](include/table.md)
        * [如何开启include功能](include/enable.md)
        * [定义自己的命令](include/commands.md)
        
    * [流式计算](stream/README.md)        
        * [数据源](stream/datasource.md)           
        * [如何将Kafka中JSON/CSV转化为表](stream/data_convert.md)
        * [window/watermark的使用](stream/window_wartermark.md) 
        * [如何使用MLSQL流式更新MySQL数据](stream/stream_mysql_update.md)                   
    
    * [特征工程组件](feature/README.md)          
      * [文本向量化](feature/nlp.md)
         * [TFIDF](feature/tfidf.md)
         * [Word2Vec](feature/word2vec.md)
      * [特征平滑](feature/scale.md)
      * [归一化](feature/normalize.md)
      * [混淆矩阵](feature/confusion_matrix.md)
      * [QQ/电话/邮件抽取](feature/some_extract.md)           
      * [离散化](feature/discretizer/README.md)
        * [Bucketizer](feature/discretizer/bucketizer.md)
        * [Quantile](feature/discretizer/quantile.md)      
      * [Map转化为向量](feature/vecmap.md)      
      * [TokenExtract / TokenAnalysis]()
      * [数据集切分](feature/rate_sample.md)                 
                       
    * [Python算法](python_alg/README.md)
        * [集成SKlearn示例](python_alg/sklearn.md)
        * [集成TensorFlow示例-待写]()
        * [TensorFlow Cluster支持-待写]()
    
    * [MLSQL内置算法](algs/README.md)
        * [NaiveBayes](algs/naive_bayes.md)
        * [ALS](algs/als.md)
        * [RandomForest](algs/random_forest.md)        
        * [LDA](algs/lda.md)        
        * [XGBoost](algs/xgboost.md)
    
    * [深度学习](dl/README.md)
        * [加载图片数据](dl/load_image.md)
        * [Cifar10示例](dl/cifar10.md)
        
    * [部署算法API服务](api_deploy/README.md)
        * [设计和原理](api_deploy/design.md)
        * [部署流程](api_deploy/case.md)        
        
    * [爬虫]()
        * [爬虫示例]()
        * [基于MLSQL爬虫系统的设计]()
    
    * [保障数据安全](security/README.md)
        * [MLSQL 编译时权限控制](security/compile-auth.md)
        * [MLSQL统一授权体系设计原理](security/design.md)
        * [如何开发自定义授权规则](security/build.md)
        * [如何管理connect账号和密码](security/user_password.md)
    
    * [MLSQL两种运行模式](mode/README.md)
        * [Service模式](mode/service.md)
        * [Application模式](mode/application.md)
            
    * [如何执行初始化脚本](application/README.md)            
        
    * [管理多个MLSQL实例](cluster/README.md)
        * [MLSQL Cluster 路由策略](cluster/route.md)    
    
    * [常见问题集锦](qa/README.md)        
        

* 开发者指南
    * [MLSQL-ET开发指南](develop/et.md) 
    * [MLSQL数据源开发指南](develop/datasource.md)
    * [Ambari HDP Spark多版本兼容](develop/ambari_multi_spark.md)
    * [如何参与开发]()
    * [开发者列表]() 

* MLSQL实战 
    * [产品和运营如何利用MLSQL完成excel处理](action/mlsql-excel.md) 
           
         
    
           
           


    
        
