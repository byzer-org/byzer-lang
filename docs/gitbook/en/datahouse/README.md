# For MLSQL warehouse/Data Lake

# Warehouse/Data lake is important for Bigdata. MLSQL supports traditional hive operations, also latest Delta. 
Meanwhile, there are several tough things for warehouse:
1. Data sync
2. Streaming support
3. Small files problem

We will elaborate it that how does MLSQL solve these problems in this chapter.
Attention！MLSQL Engine needs to access Hive, please put hive configuration files(e.g., Hive-site.xml) in SPARK_HOME/conf.
Besides, we can also use JDBC to access Hive, but the performance is worse.
 