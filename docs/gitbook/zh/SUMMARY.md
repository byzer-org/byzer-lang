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

# MLSQL Stack 手册

## 目录

* 概要
    * [MLSQL简介](getting_started/READEME.md)        

* 用户指南
    * [编译&运行&部署](installation/README.md)
        * [使用Docker安装体验](installation/docker.md)
        * [登录我们的体验站点](installation/trymlsql.md)
        * [下载预编译的安装包](installation/downloa_prebuild_package.md)
        * [自助下载源码编译打包](installation/compile.md)
    * [MLSQL语法指南](grammar/README.md)
        * [Set语法](grammar/set.md) 
        * [Load语法](grammar/load.md)
        * [Select语法](grammar/select.md)
        * [Train/Run/Predict语法](grammar/et_statement.md)
        * [Register语法](grammar/register.md)
        * [Save语法](grammar/save.md)
        * [宏语法](grammar/macro.md)
        * [Include语法](grammar/include.md)
        * [MLSQL语法解析接口](grammar/analyze.md)
    * [MLSQL Console使用指南](console/README.md)
        * [用户界面介绍](console/console_usage.md)
        * [权限和用户管理](console/auth_user.md)
        * [Notebook交互模式](console/notebook.md)
    * [MLSQL Engine多租户支持](multi_tenement/README.md)
        * [多租户相关参数](multi_tenement/conf.md)
        * [租户主目录设置](multi_tenement/home.md)
        * [租户主目录查看与管理](multi_tenement/home_fs.md)    
    * [MLSQL数仓/数据湖使用](datahouse/README.md)
        * [Hive加载和存储](datahouse/hive.md)
        * [Delta加载和存储以及流式支持](datahouse/delta.md)       
        * [MySQL Binlog同步](datahouse/binlog.md)
    * [MLSQL元信息存储](metastore/README.md)
    * [加载和存储多种数据源](datasource/README.md)      
        * [JDBC](datasource/jdbc.md)
        * [ElasticSearch](datasource/es.md)
        * [Solr](datasource/solr.md)
        * [HBase](datasource/hbase.md)
        * [MongoDB](datasource/mongodb.md)
        * [Parquet/Json/Text/Xml/Csv](datasource/file.md)
        * [jsonStr/script/mlsqlAPI/mlsqlConf](datasource/mlsql_source.md)                
    * [动态创建UDF/UDAF](udf/README.md)
        * [Python UDF](udf/python_udf.md)       
        * [Scala UDF](udf/scala_udf.md)
        * [Scala UDAF](udf/scala_udaf.md)
        * [Java UDF](udf/java_udf.md)    
    * [内置常见UDF](system_udf/README.md)
        * [http请求](system_udf/http.md)
        * [常见函数](system_udf/vec.md)                       
    * [一些有用的内置ET](process/README.md)                              
       * [计算复杂的父子关系](process/estimator_transformer/TreeBuildExt.md)
       * [改变表的分区数](process/estimator_transformer/RepartitionExt.md)
       * [如何发送邮件](process/estimator_transformer/SendMessage.md)
       * [数据采集组件mlsql-watcher使用](process/estimator_transformer/mlsql-watcher.md)
    * [如何集成调度](scheduler/README.md)
       * [内置调度](scheduler/buildin_scheduler.md)
       * [外置调度整合](scheduler/third_party_scheduler.md)
    * [如何缓存表](process/estimator_transformer/CacheExt.md)                                       
    * [MLSQL机器学习指南（Python版）](python/README.md)
        * [Python环境](python/python-env.md)
        * [使用Python做ETL](python/table.md)
        * [机器学习训练部分](python/ray.md)
        * [Python项目](python/project.md)                        
        * [TensorFlow 集群模式](python/dtf.md)
        * [如何附带资源文件](python/resource.md)
    * [MLSQL可视化](dash/README.md)
        * [把python测试数据集导出到数仓](dash/export-python-data-to-delta.md)
        * [制作一张动态报表](dash/dynamic-dash.md)
        * [制作一张交互式报表](dash/iteractive-dash.md)
    * [MLSQL插件商店](plugins/README.md)
        * [Excel数据源插件](plugins/mlsql-excel.md)
        * [Connect语句持久化](plugins/connect-persist.md)
        * [流程序持久化](plugins/stream-persist.md)
        * [将字符串当做代码执行](plugins/run-script.md)
        * [深度学习插件BigDL](plugins/mlsql-bigdl.md)
    * [MLSQL使用小技巧](include/README.md)    
        * [如何执行初始化脚本](include/init.md)    
        * [如何简化一条SQL语句](include/sql.md)
        * [SQL片段模板的使用](include/sql-snippet-template.md)
        * [如何统一管理scala/python udf脚本](include/include_script.md)        
        * [如何统一管理中间表](include/table.md)
        * [如何开启include功能](include/enable.md)
        * [定义自己的命令](include/commands.md)        
    * [流式计算](stream/README.md)               
        * [Kafka和MockStream](stream/datasource.md)
        * [MLSQL Kafka小工具集锦](stream/kakfa_tool.md)           
        * [自动将Kafka中的JSON数据展开为表](stream/data_convert.md)
        * [如何高效的AdHoc查询Kafka](stream/query_kafka.md)
        * [如何设置流式计算回调](stream/callback.md)
        * [如何对流的结果以批的形式保存](stream/batch.md)
        * [window/watermark的使用](stream/window_wartermark.md) 
        * [如何使用MLSQL流式更新MySQL数据](stream/stream_mysql_update.md)
        * [流结果输出到WebConsole](stream/web_console.md)                        
    * [特征工程](feature/README.md)          
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
        * [数据集切分](feature/rate_sample.md)                                        
    * [Python算法](python_alg/README.md)
        * [集成SKlearn示例](python_alg/sklearn.md)           
    * [MLSQL内置算法](algs/README.md)
        * [NaiveBayes](algs/naive_bayes.md)
        * [ALS](algs/als.md)
        * [RandomForest](algs/random_forest.md)        
        * [LDA](algs/lda.md)        
        * [XGBoost](algs/xgboost.md)    
    * [基于Java的深度学习框架集成](dl/README.md)
        * [加载图片数据](dl/load_image.md)
        * [Cifar10示例](dl/cifar10.md)        
    * [部署算法API服务](api_deploy/README.md)
        * [设计和原理](api_deploy/design.md)
        * [部署流程](api_deploy/case.md)                
    * [任务/资源管理](jobs/README.md)
        * [查看正在任务列表](jobs/list_jobs.md)
        * [取消正在运行任务](jobs/cancel_job.md)
        * [动态添加/删除资源](jobs/dynamic_resource.md)    
    * [保障数据安全](security/README.md)
        * [基本的格式以及术语解释](security/sumary.md)
        * [开发权限客户端插件](security/auth-client.md)
        * [ET组件的权限](security/auth-et.md)
        * [如何管理connect账号和密码](security/user_password.md)    
    * [MLSQL两种运行模式](mode/README.md)
        * [Service模式](mode/service.md)
        * [Application模式](mode/application.md)                                   
    * [管理多个MLSQL实例](cluster/README.md)
        * [MLSQL Cluster 路由策略](cluster/route.md)        
    * [常见问题集锦](qa/README.md)
    * [长时间稳定运行注意事项](qa/long-run-issues.md)        
    * [Cheatsheet](qa/cheatsheet.md)        
        

* 开发者指南
    * [自己写脚本启动MLSQL Engine](installation/run.md)
        * [启动参数说明](installation/startup-configuration.md)       
        * [MLSQL IDE开发环境配置](installation/ide.md)
    * [MLSQL ET插件 deltaenhancer 开发示例](develop/et_delta_enhancer.md)
    * [MLSQL ET插件 随机森林算法 开发示例](develop/et.md) 
    * [MLSQL DataSource插件开发指南](develop/datasource.md)
    * [MLSQL Script插件 binlog2delta 开发示例](develop/binlog2delta.md)
    * [MLSQL App插件 EchoController 开发示例](develop/echo_controller.md)
    * [Ambari HDP Spark多版本兼容](develop/ambari_multi_spark.md)  
    * [MLSQL监控-集成prometheus](develop/spark_prometheus.md)

* MLSQL实战 
    * [产品和运营如何利用MLSQL完成excel处理](action/mlsql-excel.md) 
           
         
    
           
           


    
        
