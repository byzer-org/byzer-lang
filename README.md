## MLSQL

MLSQL is a Programming Language Designed For Big Data and AI, and it also have a distributed runtime.

![](http://docs.mlsql.tech/upload_images/WechatIMG67.png)

## Official WebSite

[http://www.mlsql.tech](http://www.mlsql.tech)


Find more examples on [our user guide](http://docs.mlsql.tech/en).

1. [中文文档](http://docs.mlsql.tech/mlsql-stack/)
2. [English Docs](http://docs.mlsql.tech/en)


## Get PreBuild Distribution

* The lasted version is MLSQL v2.0.1
* You can download from [MLSQL Website](http://download.mlsql.tech/2.0.1/)
* Spark 2.4.3/3.0.0 are tested


Run PreBuild Distribution:

```shell
cp streamingpro-spark_2.x-x.x.x.tar.gz /tmp
cd /tmp && tar xzvf  streamingpro-spark_2.x-x.x.x.tar.gz
cd /tmp/streamingpro-spark_2.x-x.x.x

## make sure spark distribution is available
## visit http://127.0.0.1:9003
export SPARK_HOME="....." ; ./start-default.sh
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

## Fork and Contribute

If you are planning to contribute to this repository, we first request you to create an issue at [our Issue page](https://github.com/allwefantasy/streamingpro/issues)
even if the topic is not related to source code itself (e.g., documentation, new idea and proposal).

This is an active open source project for everyone,
and we are always open to people who want to use this system or contribute to it.


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
* Slash-Wong/523935329#qq.com
* anan0120/158989903#qq.com


----------
[![HitCount](http://hits.dwyl.io/allwefantasy/streamingpro.svg)](http://hits.dwyl.io/allwefantasy/streamingpro)

----------

##  WeChat Group

扫码添加小可爱微信号，添加成功后，发送  mlsql  这5个英文字母进群。

![](http://docs.mlsql.tech/upload_images/WX20200505-090326@2x.png)

