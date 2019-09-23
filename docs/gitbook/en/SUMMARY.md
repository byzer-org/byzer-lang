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

# MLSQL Stack User manual             

## List

* outline
    * [MSLQL brief introduction](getting_started/READEME.md)        

* User guide
    * [Compile & Run & Deploy](installation/README.md)
        * [Docker installation and experience](installation/docker.md)
        * [Official website experience site](installation/trymlsql.md)
        * [Download precompiled installer packages](installation/downloa_prebuild_package.md)
        * [Source code compilation and packaging](installation/compile.md)
    * [MLSQL grammar usage guide](grammar/README.md)
        * [Set](grammar/set.md) 
        * [Load](grammar/load.md)
        * [Select](grammar/select.md)
        * [Train & Run & Predict](grammar/et_statement.md)
        * [Register](grammar/register.md)
        * [Save](grammar/save.md)
        * [Macro](grammar/macro.md)
        * [Include](grammar/include.md)
        * [MLSQL grammar parsing interface](grammar/analyze.md)
    * [MLSQL Console usage guide](console/README.md)
        * [User interface introduction](console/console_usage.md)
        * [Authority and user management](console/auth_user.md)
        * [Notebook interaction mode](console/notebook.md)
    * [MLSQL Engine multi tenant](multi_tenement/README.md)
        * [Related parameters](multi_tenement/conf.md)
        * [Main directory settings](multi_tenement/home.md)
        * [Main directory view and management](multi_tenement/home_fs.md)    
    * [MLSQL datawarehousr and datalake usage](datahouse/README.md)
        * [Hive loading and storage](datahouse/hive.md)
        * [Delta loading & storage & Streaming](datahouse/delta.md)       
        * [MySQL Binlog](datahouse/binlog.md)                                                                               
    * [Loading and Storage multiple data sources](datasource/README.md)      
        * [JDBC](datasource/jdbc.md)
        * [ElasticSearch](datasource/es.md)
        * [Solr](datasource/solr.md)
        * [HBase](datasource/hbase.md)
        * [MongoDB](datasource/mongodb.md)
        * [Parquet/Json/Text/Xml/Csv](datasource/file.md)
        * [jsonStr/script/mlsqlAPI/mlsqlConf](datasource/mlsql_source.md)        
        * [Custom data source](datasource/other.md)
        * [Add data source dependency at runtime](datasource/dynamically_add.md)
    * [Creating UDF/UDAF dynamically](udf/README.md)
        * [Python UDF](udf/python_udf.md)       
        * [Scala UDF](udf/scala_udf.md)
        * [Scala UDAF](udf/scala_udaf.md)
        * [Java UDF](udf/java_udf.md)    
    * [Common built-in UDF](system_udf/README.md)
        * [http request](system_udf/http.md)
        * [Common functions](system_udf/vec.md)                       
    * [Some useful built-ins ET](process/README.md)                              
       * [Calculating complex paternity](process/estimator_transformer/TreeBuildExt.md)
       * [Change partition number of table](process/estimator_transformer/RepartitionExt.md)
       * [How to send mail](process/estimator_transformer/SendMessage.md) 
    * [How to integrate scheduling](scheduler/README.md)
       * [Use built-in scheduling](scheduler/buildin_scheduler.md)
    * [How to cache table](process/estimator_transformer/CacheExt.md)                                       
    * [Python Support](python/README.md)
        * [Python environment](python/python-env.md)        
        * [Interactive Python](python/interactive.md)
        * [Use Python to process tables in MLSQL](python/table.md)
        * [Python project](python/project.md)                
        * [Run Python projects in a distributed environment](python/distribute-python.md)
        * [Run a single instance Python project](python/python.md)
        * [TensorFlow cluster mode](python/dtf.md)
        * [How to attach resource documents](python/resource.md)
    * [MLSQL plugins store](plugins/README.md)          
    * [MLSQL use tips](include/README.md)    
        * [How to execute initialization script](include/init.md)    
        * [How to simplify a SQL statement](include/sql.md)        
        * [How to manage scala/python udf scripts uniformly](include/include_script.md)        
        * [How to manage the middle table uniformly](include/table.md)
        * [How to turn on the include function](include/enable.md)
        * [How to customize commands](include/commands.md)        
    * [Streaming Computing](stream/README.md)        
        * [Kafka and MockStream](stream/datasource.md)
        * [Toolkit using MLSQL Kafka](stream/kakfa_tool.md)           
        * [Convert JSON data in Kafka to table](stream/data_convert.md)
        * [How to AdHoc Kafka efficiently](stream/query_kafka.md)
        * [How to Set Streaming Computing Callback](stream/callback.md)
        * [How to batch save Streaming computing results](stream/batch.md)
        * [Use window/watermark](stream/window_wartermark.md) 
        * [How to update MySQL data using MLSQL streaming](stream/stream_mysql_update.md)                       
    * [Feature Engineering](feature/README.md)          
        * [Text vectorization](feature/nlp.md)
            * [TFIDF](feature/tfidf.md)
            * [Word2Vec](feature/word2vec.md)
        * [Feature smoothing](feature/scale.md)
        * [Normalization](feature/normalize.md)
        * [Confusion matrix](feature/confusion_matrix.md)
        * [QQ/Phone/mail extract](feature/some_extract.md)           
        * [Discretization](feature/discretizer/README.md)
            * [Bucketizer](feature/discretizer/bucketizer.md)
            * [Quantile](feature/discretizer/quantile.md)      
        * [Map to vector](feature/vecmap.md)              
        * [Data segmentation](feature/rate_sample.md)                                        
    * [Python algorithm](python_alg/README.md)
        * [Integrated SKlearn example](python_alg/sklearn.md)           
    * [MLSQL built-in algorithms](algs/README.md)
        * [NaiveBayes](algs/naive_bayes.md)
        * [ALS](algs/als.md)
        * [RandomForest](algs/random_forest.md)        
        * [LDA](algs/lda.md)        
        * [XGBoost](algs/xgboost.md)    
    * [Deep learning](dl/README.md)
        * [Load image data](dl/load_image.md)
        * [Cifar10 example](dl/cifar10.md)        
    * [Deploy algorithm API service](api_deploy/README.md)
        * [Design and principle](api_deploy/design.md)
        * [Deployment process](api_deploy/case.md)                
    * [Task & resource management](jobs/README.md)
        * [View a list of running tasks](jobs/list_jobs.md)
        * [Cancel the list of running tasks](jobs/cancel_job.md)
        * [Dynamically add & remove resources](jobs/dynamic_resource.md)    
    * [Data security](security/README.md)
        * [MLSQL language level permission control](security/compile-auth.md)
        * [MLSQL unified authorization system design principles](security/design.md)
        * [How to develop custom authorization rules](security/build.md)
        * [How to manage connect account and password](security/user_password.md)    
    * [MLSQL two operating modes](mode/README.md)
        * [Service mode](mode/service.md)
        * [Application mode](mode/application.md)                                   
    * [Manage multiple MLSQL instances](cluster/README.md)
        * [MLSQL Cluster routing strategy](cluster/route.md)        
    * [FAQ](qa/README.md)        
        

* Developer Quick Guide
    * [Custom script startup MLSQL Engine](installation/run.md)
        * [Startup parameter description](installation/startup-configuration.md)       
        * [MLSQL IDE development environment configuration](installation/ide.md)
    * [MLSQL ET plugin , deltaenhancer developed example](develop/et_delta_enhancer.md)
    * [MLSQL ET plugin , Random Forest algorithm developed examples](develop/et.md) 
    * [MLSQL DataSource plugin developed guide](develop/datasource.md)
    * [MLSQL Script plugin , binlog2delta developed examples](develop/binlog2delta.md)
    * [Ambari HDP Spark Multi-version compatible](develop/ambari_multi_spark.md)     

* MLSQL action 
    * [How products and operations use MLSQL to complete excel processing](action/mlsql-excel.md) 
           
         
    
           
           


    
        
