## MLSQL v1.1.6 Release

We are glad to announce that  [MLSQL v1.1.6](http://download.mlsql.tech/mlsql-1.1.6) is released.  There are huge  new features, bug fix and code refractor in this  half month' development:

1. Almost 163 commits.
2. 28 [ISSUE](https://github.com/allwefantasy/streamingpro/issues?page=1&q=is%3Aissue+is%3Aclosed+label%3AMLSQL-1.1.6)
3. 41 [PR](https://github.com/allwefantasy/streamingpro/pulls?q=is%3Apr+is%3Aclosed+label%3AMLSQL-1.1.6). 25 of them are BugFix, 26 of them are improvements.

## Improvements：

* [PR-747](https://github.com/allwefantasy/streamingpro/pull/744):  Load balance, Dynamic Resource Allocation,Instances manager based on labels。More details please check：[mlsql-cluster doc](https://github.com/allwefantasy/streamingpro/blob/master/docs/docv2/cluster/cluster.md).
* [PR-735](https://github.com/allwefantasy/streamingpro/pull/735): Supports Using load statement to view API doc and Configuration of MLSQL。
* [PR-728](https://github.com/allwefantasy/streamingpro/pull/728): Upgrade to latest carbondata 
* [PR-781](https://github.com/allwefantasy/streamingpro/pull/781): A new transformer supports complex father-children relation computing which is hard if you use SQL to do this.
* [PR-794](https://github.com/allwefantasy/streamingpro/pull/794):  Provides shell/cmd script to make you compile/package MLSQL more easy.

## BugFix：

* [PR-773](https://github.com/allwefantasy/streamingpro/pull/773)： The last As keyword is upper will cause exception。
* [PR-777](https://github.com/allwefantasy/streamingpro/pull/777): Save partition by can not support multi columns.
* [PR-760](https://github.com/allwefantasy/streamingpro/pull/760):  conflict in multi kafka-client.（MLSQLsupport kafka 0.8/0.9/1.x at the same time）
* [PR-757](https://github.com/allwefantasy/streamingpro/pull/757): MLSQL Auth bug
* [PR-795](https://github.com/allwefantasy/streamingpro/pull/795)： PS cluster enabled will fail the exetuor in yarn mode.

## Code refractor：

* [PR-727](https://github.com/allwefantasy/streamingpro/pull/727)： Remove the support for Spark 1.6.x
* [PR-732](https://github.com/allwefantasy/streamingpro/pull/732)： Move some non-core modules to external/contri directory.







