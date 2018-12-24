## MLSQL

MLSQL is a Language which is the superset of SQL and  a distributed compute engine based on Spark. 

With MLSQL,you can unifies big data and machine learning,one language, one platform.

Notice that with the power of [UDF/Script](https://github.com/allwefantasy/streamingpro/blob/master/docs/en/mlsql-script-support.md) supports,no matter where your data is, what APIs you have, 
you can use them in MLSQL script to make your job more easy. 

## Usage:

![](https://github.com/allwefantasy/streamingpro/raw/master/images/WX20181106-164911.png)

Find more examples on [our user guide](https://github.com/allwefantasy/streamingpro/blob/master/docs/docv2/user-guide.md).

## Fork and Contribute

If you are planning to contribute to this repository, we first request you to create an issue at [our Issue page](https://github.com/allwefantasy/streamingpro/issues)
even if the topic is not related to source code itself (e.g., documentation, new idea and proposal).

This is an active open source project for everyone,
and we are always open to people who want to use this system or contribute to it.
This guide document introduce [how to contribute to MLSQL](https://github.com/allwefantasy/streamingpro/blob/master/docs/docv2/contribute/contribute.md).

## RoadMap

1. [MLSQL v1.1.7 RoadMap](https://www.jianshu.com/p/622ff0c7beb1) MLSQL v1.1.7 plans to release in Mid Jan 2019 
2. [Versioning Policy](https://github.com/allwefantasy/streamingpro/blob/master/docs/docv2/contribute/release.md)



## Get PreBuild Distribution

* [Release Notes](https://github.com/allwefantasy/streamingpro/tree/master/docs/docv2/release_notes)
* [Releases Page](https://github.com/allwefantasy/streamingpro/releases)
* The lasted version is [MLSQL-v1.1.6](https://github.com/allwefantasy/streamingpro/releases/tag/v1.1.6).
* You can also download from [MLSQL Website](http://download.mlsql.tech/mlsql-1.1.6/)


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
export MLSQL_SPARK_VERSIOIN=2.3
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

