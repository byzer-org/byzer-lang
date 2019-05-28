## Ambari hdp Spark多版本兼容

本示例基于`ambari hdp-2.6.0.3`（`spark2.1.0`），集成`spark2.2.2`，其他的版本思路类似（`spark2.3`、`spark2.4`经测试也是没问题的）


1. 下载`spark-2.2.2-bin-hadoop2.7.tgz`,并解压到放到任意目录;

2. 修改配置

	```
	cd spark-2.2.2-bin-hadoop2.7/
	1. 复制ambari的hive-site.xml到conf
		cp /usr/hdp/2.6.0.3-8/hive/conf/hive-site.xml .

	2. 复制ambari的spark-defaults.conf到conf
		cp /usr/hdp/2.6.0.3-8/spark2/conf/spark-defaults.conf .
		并修改：
		spark.history.fs.logDirectory hdfs:///spark2.2.2-history/
		spark.yarn.historyServer.address [ip/domain name]:18081
		spark.driver.extraJavaOptions -Dhdp.version=2.6.0.3-8
		spark.yarn.am.extraJavaOptions -Dhdp.version=2.6.0.3-8

	3. 复制ambari的spark-env.sh到conf
		cp /usr/hdp/2.6.0.3-8/spark2/conf/spark-env.sh .
		并修改：
		export SPARK_CONF_DIR=${SPARK_CONF_DIR:-/home/install/spark-2.2.2-bin-hadoop2.7/conf}
		export SPARK_LOG_DIR=/var/log/spark2.2.2
		export SPARK_PID_DIR=/var/run/spark2.2.2

	4. 解决jersey版本冲突
		cd spark-2.2.2-bin-hadoop2.7/jars
		mv jersey-client-2.22.2.jar jersey-client-2.22.2.jar.bak
		cp /usr/hdp/2.6.0.3-8/hadoop-yarn/lib/jersey-client-1.9.jar .
		cp /usr/hdp/2.6.0.3-8/hadoop-yarn/lib/jersey-core-1.9.jar .
	```
	备注：为了与ambari原生spark分离，因此新建`Spark2 History Server`目录与日志目录；`[ip/domain name]`为启动新版本的`Spark2 History Server`机器ip地址或域名;

3. 为保证`yarn cluster`模式可以正常运行需要做如下修改：

	```
	进入Ambari管理页面 -> MapReduce2 -> Configs -> advanced -> Advanced mapred-site

	把所有的${hdp.version}改成2.6.0.3-8
	```

4. 在启动`Spark2 History Server`的服务器创建文件：

	```
	1. 创建日志目录与pid目录
		cd /var/log
		mkdir spark2.2.2
		chown spark:hadoop spark2.2.2

		cd /var/run
		mkdir spark2.2.2
		chown spark:hadoop spark2.2.2

	2. 创建hdfs目录
		hadoop fs -mkdir /spark2.2.2-history
		hadoop fs -chown spark:hadoop /spark2.2.2-history
		hadoop fs -chmod 777 /spark2.2.2-history
	```

5. 如果yarn的`ResourceManager`是`HA`模式需要如下修改：

	```
	cd spark-2.2.2-bin-hadoop2.7/jars
	mv hadoop-auth-2.7.3.jar hadoop-auth-2.7.3.jar.bak
	mv hadoop-common-2.7.3.jar hadoop-common-2.7.3.jar.bak
	mv hadoop-yarn-api-2.7.3.jar hadoop-yarn-api-2.7.3.jar.bak
	mv hadoop-yarn-client-2.7.3.jar hadoop-yarn-client-2.7.3.jar.bak
	mv hadoop-yarn-common-2.7.3.jar hadoop-yarn-common-2.7.3.jar.bak
	mv hadoop-yarn-server-common-2.7.3.jar hadoop-yarn-server-common-2.7.3.jar.bak

	cp /usr/hdp/2.6.0.3-8/hadoop-yarn/hadoop-yarn-api-2.7.3.2.6.0.3-8.jar .
	cp /usr/hdp/2.6.0.3-8/hadoop-yarn/hadoop-yarn-client-2.7.3.2.6.0.3-8.jar .
	cp /usr/hdp/2.6.0.3-8/hadoop-yarn/hadoop-yarn-common-2.7.3.2.6.0.3-8.jar .
	cp /usr/hdp/2.6.0.3-8/hadoop-yarn/hadoop-yarn-server-common-2.7.3.2.6.0.3-8.jar .
	cp /usr/hdp/2.6.0.3-8/hadoop/hadoop-common-2.7.3.2.6.0.3-8.jar .
	cp /usr/hdp/2.6.0.3-8/hadoop/hadoop-auth-2.7.3.2.6.0.3-8.jar .
或者
	cp /usr/hdp/current/spark2-client/jars/hadoop-yarn-api-2.7.3.2.6.0.3-8.jar .
	cp /usr/hdp/current/spark2-client/jars/hadoop-yarn-client-2.7.3.2.6.0.3-8.jar .
	cp /usr/hdp/current/spark2-client/jars/hadoop-yarn-common-2.7.3.2.6.0.3-8.jar .
	cp /usr/hdp/current/spark2-client/jars/hadoop-yarn-server-common-2.7.3.2.6.0.3-8.jar .
	cp /usr/hdp/current/spark2-client/jars/hadoop-common-2.7.3.2.6.0.3-8.jar .
	cp /usr/hdp/current/spark2-client/jars/hadoop-auth-2.7.3.2.6.0.3-8.jar .
	```

6. 启动新版本的`Spark2 History Server`

7. 用新版本的`spark-submit`提交程序