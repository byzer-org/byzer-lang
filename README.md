## MLSQL - A cross,distributed platform unions BigData and AI.

MLSQL is also  a Language akin to SQL which you can use to do batch, stream,crawler and AI.

## Usage:

![](https://github.com/allwefantasy/streamingpro/raw/master/images/WX20181106-164911.png)

Find more examples on [our user guide](https://github.com/allwefantasy/streamingpro/blob/master/docs/docv2/user-guide.md).

## Fork and Contribute

If you are planning to contribute to this repository, we first request you to create an issue at [our Issue page](https://github.com/allwefantasy/streamingpro/issues)
even if the topic is not related to source code itself (e.g., documentation, new idea and proposal).

This is an active open source project for everyone,
and we are always open to people who want to use this system or contribute to it.
This guide document introduce [how to contribute to MLSQL](https://github.com/allwefantasy/streamingpro/blob/master/docs/docv2/contribute/contribute.md).

## Roadmap

1. [Roadmap](https://github.com/allwefantasy/streamingpro/blob/master/docs/docv2/contribute/roadmap.md)
2. [Versioning Policy](https://github.com/allwefantasy/streamingpro/blob/master/docs/docv2/contribute/release.md)


## Get PreBuild Distribution

[Releases Page](https://github.com/allwefantasy/streamingpro/releases)
You also can refer this: [Compile Assistance](https://streamingpro.rebiekong.com/index.html)

## Build Distribution


```
git clone https://github.com/allwefantasy/streamingpro .
cd streamingpro

export MLSQL_SPARK_VERSIOIN=2.3;export DRY_RUN=false && ./dev/make-distribution.sh


cp streamingpro-bin-x.x.x.tgz /tmp
cd /tmp && tar xzvf  streamingpro-bin-x.x.x.tgz
cd /tmp/streamingpro

export SPARK_HOME="....." ; ./start-local.sh
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

