# ************************************************************
# Sequel Pro SQL dump
# Version 4096
#
# http://www.sequelpro.com/
# http://code.google.com/p/sequel-pro/
#
# Host: 127.0.0.1 (MySQL 5.6.24)
# Database: spark_jobs
# Generation Time: 2017-08-20 13:02:42 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table t_params_conf
# ------------------------------------------------------------

DROP TABLE IF EXISTS `t_params_conf`;

CREATE TABLE `t_params_conf` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `params` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table t_spark_application
# ------------------------------------------------------------

DROP TABLE IF EXISTS `t_spark_application`;

CREATE TABLE `t_spark_application` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `applicationId` text,
  `parentApplicationId` text,
  `url` text,
  `source` text,
  `beforeShell` text,
  `afterShell` text,
  `keepRunning` int(11) DEFAULT NULL,
  `watchInterval` int(11) DEFAULT NULL,
  `startTime` bigint(20) DEFAULT NULL,
  `endTime` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table t_spark_application_log
# ------------------------------------------------------------

DROP TABLE IF EXISTS `t_spark_application_log`;

CREATE TABLE `t_spark_application_log` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `appId` int(11) unsigned NOT NULL,
  `applicationId` text,
  `url` text,
  `source` text,
  `parentApplicationId` text,
  `failReason` text,
  `startTime` bigint(20) NOT NULL,
  `endTime` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table t_spark_jar
# ------------------------------------------------------------

DROP TABLE IF EXISTS `t_spark_jar`;

CREATE TABLE `t_spark_jar` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(512) DEFAULT NULL,
  `path` text,
  `createTime` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table t_spark_job_parameter
# ------------------------------------------------------------

DROP TABLE IF EXISTS `t_spark_job_parameter`;

CREATE TABLE `t_spark_job_parameter` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` text,
  `parameterType` text,
  `priority` int(11) unsigned NOT NULL,
  `label` text,
  `description` text,
  `app` text,
  `actionType` text,
  `value` text,
  `formType` text,
  `comment` text,
  `parentName` text NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `t_spark_job_parameter` WRITE;
/*!40000 ALTER TABLE `t_spark_job_parameter` DISABLE KEYS */;

INSERT INTO `t_spark_job_parameter` (`id`, `name`, `parameterType`, `priority`, `label`, `description`, `app`, `actionType`, `value`, `formType`, `comment`, `parentName`)
VALUES
	(1,'mmspark.name','string',1,'Yarn上显示名','','spark','normal','','normal','',''),
	(2,'mmspark.streaming.name','string',1,'SparkUI显示名称','','spark','normal','','normal','',''),
	(3,'mmspark.main_jar','string',1,'jar包选择','','jar','select','','select','',''),
	(4,'mmspark.streaming.job.file.path','string',1,'配置文件','','spark','normal','','normal','',''),
	(5,'mmspark.streaming.kafka.offsetPath','string',1,'Kafka offset 保存目录','','spark','normal','','normal','','mmspark.streaming.platform'),
	(6,'mmspark.streaming.platform','string',1,'平台','','spark','select','','select','',''),
	(7,'mmspark.streaming.rest','string',1,'打开http接口','','spark','select','','select','',''),
	(8,'mmspark.streaming.spark.service','string',1,'后台服务','','spark','select','','select','',''),
	(9,'mmspark.streaming.driver.port','string',1,'http端口','','spark','normal','9003','normal','','mmspark.streaming.rest'),
	(10,'mmspark.streaming.enableHiveSupport','string',1,'启用Hive支持','','spark','select','','select','',''),
	(11,'mmspark.streaming.job.cancel','string',1,'启用查询超时功能','','spark','select','','select','',''),
	(12,'mmspark.master','string',2,'master','','spark','select','','select','',''),
	(13,'mmspark.deploy-mode','string',2,'deploy-mode','','spark','select','','select','',''),
	(14,'mmspark.spark.shuffle.manager','string',2,'shuffle管理器','','spark','select','','select','',''),
	(15,'mmspark.spark.storage.memoryFraction','string',2,'存储内存比例','','spark','select','','select','',''),
	(16,'mmspark.executor-cores','string',2,'单Executor 核数','','spark','select','','select','',''),
	(17,'mmspark.spark.locality.wait','string',2,'本地性','','spark','select','','select','',''),
	(18,'mmspark.spark.speculation','string',2,'speculation','','spark','select','','select','',''),
	(19,'mmspark.spark.speculation.quantile','string',2,'speculation.quantile','','spark','normal','','normal','','mmspark.spark.speculation'),
	(20,'mmspark.spark.speculation.interval','string',2,'speculation.interval','','spark','normal','','normal','','mmspark.spark.speculation'),
	(21,'mmspark.spark.speculation.multiplier','string',2,'speculation.multiplier','','spark','normal','','normal','','mmspark.spark.speculation'),
	(22,'mmspark.spark.dynamicAllocation.enabled','string',2,'dynamicAllocation','','spark','select','','select','',''),
	(23,'mmspark.spark.sql.shuffle.partitions','string',2,'shuffle分区数','','spark','normal','9','normal','',''),
	(24,'mmspark.spark.kryoserializer.buffer.max','string',2,'kryoserializer buffer','','spark','normal','1024m','normal','',''),
	(25,'mmspark.spark.serializer','string',2,'序列化','kyro更快，但通用性略差','spark','select','','select','',''),
	(26,'mmspark.spark.io.compression.codec','string',2,'压缩方式','','spark','select','','select','',''),
	(27,'mmspark.spark.executor.extraJavaOptions','string',2,'Executor JVM配置','','-','normal','-XX:MaxPermSize=512m -XX:PermSize=128m','normal','',''),
	(28,'mmspark.spark.driver.extraJavaOptions','string',2,'Driver JVM配置','','-','normal','-XX:MaxPermSize=512m -XX:PermSize=128m','normal','',''),
	(29,'AppType','string',2,'应用类型','','spark','select','','select','',''),
	(30,'mmspark.class','string',3,'启动类','','-','normal','streaming.core.StreamingApp','normal','',''),
	(31,'mmspark.num-executors','string',3,'Executor数','','-','normal','1','normal','',''),
	(32,'mmspark.executor-memory','string',3,'Executor内存','','-','normal','3g','normal','',''),
	(33,'mmspark.driver-memory','string',3,'Driver内存','','-','normal','4g','normal','',''),
	(34,'mmspark.args','string',3,'应用参数','应用的额外参数，譬如如果需要升级等需要加-upgrade参数等','-','normal','','normal','','');

/*!40000 ALTER TABLE `t_spark_job_parameter` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table t_spark_job_parameter_values
# ------------------------------------------------------------

DROP TABLE IF EXISTS `t_spark_job_parameter_values`;

CREATE TABLE `t_spark_job_parameter_values` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` text,
  `formType` int(11) DEFAULT NULL,
  `value` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

LOCK TABLES `t_spark_job_parameter_values` WRITE;
/*!40000 ALTER TABLE `t_spark_job_parameter_values` DISABLE KEYS */;

INSERT INTO `t_spark_job_parameter_values` (`id`, `name`, `formType`, `value`)
VALUES
	(1,'mmspark.spark.shuffle.manager',0,'tungsten-sort'),
	(2,'mmspark.spark.storage.memoryFraction',0,'0.1,0.3,0.05'),
	(3,'mmspark.executor-cores',0,'2,3'),
	(4,'mmspark.spark.locality.wait',0,'0ms,10ms,50ms'),
	(5,'mmspark.spark.serializer',0,'org.apache.spark.serializer.KryoSerializer,org.apache.spark.serializer.JavaSerializer'),
	(6,'mmspark.spark.io.compression.codec',0,'org.apache.spark.io.LZ4CompressionCodec,org.apache.spark.io.SnappyCompressionCodec'),
	(7,'mmspark.streaming.platform',0,'spark,ss,spark_streaming,flink'),
	(8,'mmspark.spark.dynamicAllocation.enabled',0,'false,true'),
	(9,'mmspark.streaming.job.cancel',0,'false,true'),
	(10,'mmspark.spark.speculation',0,'false,true'),
	(11,'mmspark.streaming.rest',0,'false,true'),
	(12,'mmspark.spark.yarn.executor.memo',0,'false,true'),
	(13,'mmspark.streaming.spark.service',0,'false,true'),
	(14,'mmspark.streaming.enableHiveSupport',0,'false,true'),
	(15,'mmspark.master',0,'yarn,local[1],local[2],local[4]'),
	(16,'mmspark.deploy-mode',0,'cluster,client'),
	(17,'AppType',0,'spark');

/*!40000 ALTER TABLE `t_spark_job_parameter_values` ENABLE KEYS */;
UNLOCK TABLES;


# Dump of table t_test_conf
# ------------------------------------------------------------

DROP TABLE IF EXISTS `t_test_conf`;

CREATE TABLE `t_test_conf` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` text,
  `user` text,
  `url` text,
  `host` text,
  `insert_time` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
