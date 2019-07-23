## MLSQL

MLSQL is a Language which is the superset of SQL and  a distributed compute engine based on Spark. 

## Official WebSite

[http://www.mlsql.tech](http://www.mlsql.tech)


Find more examples on [our user guide](http://docs.mlsql.tech/en).

1. [中文文档](http://docs.mlsql.tech/zh)
2. [English Docs](http://docs.mlsql.tech/en)

## Fork and Contribute

If you are planning to contribute to this repository, we first request you to create an issue at [our Issue page](https://github.com/allwefantasy/streamingpro/issues)
even if the topic is not related to source code itself (e.g., documentation, new idea and proposal).

This is an active open source project for everyone,
and we are always open to people who want to use this system or contribute to it.
This guide document introduce [how to contribute to MLSQL](https://github.com/allwefantasy/streamingpro/blob/master/docs/docv2/contribute/contribute.md).

## RoadMap

1. [Versioning Policy](https://github.com/allwefantasy/streamingpro/blob/master/docs/docv2/contribute/release.md)


## Get PreBuild Distribution

* The lasted version is MLSQL v1.3.0
* You can download from [MLSQL Website](http://download.mlsql.tech/)
* Spark 2.3.2/2.4.3 are tested


Run PreBuild Distribution:

```shell
cp streamingpro-spark_2.x-x.x.x.tar.gz /tmp
cd /tmp && tar xzvf  streamingpro-spark_2.x-x.x.x.tar.gz
cd /tmp/streamingpro-spark_2.x-x.x.x

## make sure spark distribution is available
## visit http://127.0.0.1:9003
export SPARK_HOME="....." ; ./start-local.sh
```

## Build Distribution


```shell
# clone project
git clone https://github.com/allwefantasy/streamingpro .
cd streamingpro

## configure build envs
export MLSQL_SPARK_VERSIOIN=2.4
export DRY_RUN=false 
export DISTRIBUTION=false

## build  
./dev/package.sh
```

## Contributors

* Zhu William/allwefantasy#gmail.com
* Chen Fu/cfmcgrady#gmail.com
* Geng Yifei/pigeongeng#gmail.com
* wanp1989/wanp1989#126.com
* chenliang613
* RebieKong
* CoderOverflow
* ghsuzzy
* xubo245
* zhuohuwu0603
* liyubin117
* 9bbword


----------
[![HitCount](http://hits.dwyl.io/allwefantasy/streamingpro.svg)](http://hits.dwyl.io/allwefantasy/streamingpro)

----------

## ChatRoom

![](http://docs.mlsql.tech/upload_images/qq_small.png) 
![](http://docs.mlsql.tech/upload_images/dingding_small.png)


