# MLSQL数仓/数据湖使用

做大数据，目前来说还离不开数仓或者数据湖。 MLSQL 支持传统的Hive操作,也支持最新的Delta.在这个章节，
我们会详细阐述如何使用他们。

值得注意的是，MLSQL Engine需要能够访问Hive,最简单的办法是将hive相关的配置文件(如hive-site.xml)放到 SPARK_HOME/conf目录下。
另外，我们可以通过JDBC来访问Hive,但性能可能比较地下。