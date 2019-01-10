# MLSQL-Engine

MLSQL-Engine 是一个分布式执行引擎，可以解释执行MLSQL 脚本。用户可以使用http协议和MLSQL-Engine进行交互。
MLSQL-Engine 内核为Spark，所以它可以运行在多个平台上，比如Yarn,Mesos,K8s,StandAlone. 
启动上我们遵循了spark-submit的标准。
