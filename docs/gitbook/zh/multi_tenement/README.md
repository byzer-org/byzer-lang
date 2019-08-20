# MLSQL Engine多租户支持

当你使用MLSQL Console时，默认已经开启了MLSQL Engine的多租户支持。一旦MLSQL Engine多租户支持开启，
你讲获得如下特性：

1. 多租户共享MLSQL Engine所有计算资源
2. 不同租户临时表互相不可见
3. 每个租户都有自己独有的主目录
4. 不同租户创建的UDF 函数互不可见


在本章节中，我们假定用户不一定使用MLSQL Console,如果用户想自己开发Console,那么就需要知道如何和MLSQL Engine配合，
如何开启多租户特性。