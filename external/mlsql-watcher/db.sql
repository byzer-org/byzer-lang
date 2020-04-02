# ************************************************************
# Sequel Pro SQL dump
# Version 4096
#
# http://www.sequelpro.com/
# http://code.google.com/p/sequel-pro/
#
# Host: 127.0.0.1 (MySQL 5.7.28-log)
# Database: app_runtime_full
# Generation Time: 2020-03-13 01:56:33 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table w_executor
# ------------------------------------------------------------

DROP TABLE IF EXISTS `w_executor`;

CREATE TABLE `w_executor` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `cluster_name` varchar(256) DEFAULT NULL,
  `name` varchar(256) DEFAULT NULL,
  `host_port` varchar(256) DEFAULT NULL,
  `total_shuffle_read` bigint(20) DEFAULT NULL,
  `total_shuffle_write` bigint(20) DEFAULT NULL,
  `gc_time` bigint(20) DEFAULT NULL,
  `add_time` bigint(20) DEFAULT NULL,
  `remove_time` bigint(20) DEFAULT NULL,
  `created_at` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table w_executor_job
# ------------------------------------------------------------

DROP TABLE IF EXISTS `w_executor_job`;

CREATE TABLE `w_executor_job` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `cluster_name` varchar(256) DEFAULT NULL,
  `group_id` varchar(256) DEFAULT NULL,
  `executor_name` varchar(256) DEFAULT NULL,
  `disk_bytes_spilled` bigint(20) DEFAULT NULL,
  `shuffle_remote_bytes_read` bigint(20) DEFAULT NULL,
  `shuffle_local_bytes_read` bigint(20) DEFAULT NULL,
  `shuffle_records_read` bigint(20) DEFAULT NULL,
  `shuffle_bytes_written` bigint(20) DEFAULT NULL,
  `shuffle_records_written` bigint(20) DEFAULT NULL,
  `add_time` bigint(20) DEFAULT NULL,
  `remove_time` bigint(20) DEFAULT NULL,
  `created_at` bigint(20) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `group_id` (`group_id`),
  KEY `created_at` (`created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


# Dump of table w_kv
# ------------------------------------------------------------

DROP TABLE IF EXISTS `w_kv`;

CREATE TABLE `w_kv` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(256) DEFAULT '',
  `value` text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name_2` (`name`),
  KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
