# ************************************************************
# Sequel Pro SQL dump
# Version 4096
#
# http://www.sequelpro.com/
# http://code.google.com/p/sequel-pro/
#
# Host: 127.0.0.1 (MySQL 5.6.24)
# Database: spark_jobs
# Generation Time: 2017-07-13 10:24:37 +0000
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
