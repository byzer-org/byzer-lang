# ************************************************************
# Sequel Pro SQL dump
# Version 4096
#
# http://www.sequelpro.com/
# http://code.google.com/p/sequel-pro/
#
# Host: 127.0.0.1 (MySQL 5.7.28-log)
# Database: app_runtime_full
# Generation Time: 2020-04-06 07:19:30 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table w_app_record
# ------------------------------------------------------------

CREATE TABLE `w_app_record` (
  `plugin_name` varchar(256) DEFAULT NULL,
  `class_name` varchar(256) DEFAULT NULL,
  `params` text,
  `version` varchar(256) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table w_dependency_jobs
# ------------------------------------------------------------

CREATE TABLE `w_dependency_jobs` (
  `owner` varchar(256) DEFAULT NULL,
  `dependency` int(11) DEFAULT NULL,
  `id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table w_dict_store
# ------------------------------------------------------------

CREATE TABLE `w_dict_store` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(256) DEFAULT NULL,
  `value` text,
  `dict_type` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table w_ds_record
# ------------------------------------------------------------

CREATE TABLE `w_ds_record` (
  `plugin_name` varchar(256) DEFAULT NULL,
  `short_format` varchar(1024) DEFAULT NULL,
  `full_format` varchar(1024) DEFAULT NULL,
  `version` varchar(256) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table w_et_record
# ------------------------------------------------------------

DROP TABLE IF EXISTS `w_et_record`;

CREATE TABLE `w_et_record` (
  `plugin_name` varchar(256) DEFAULT NULL,
  `command_name` varchar(1024) DEFAULT NULL,
  `et_name` varchar(1024) DEFAULT NULL,
  `class_name` varchar(1024) DEFAULT NULL,
  `version` varchar(256) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table w_plugins
# ------------------------------------------------------------

CREATE TABLE `w_plugins` (
  `plugin_name` varchar(256) DEFAULT NULL,
  `path` varchar(1024) DEFAULT NULL,
  `plugin_type` varchar(256) DEFAULT NULL,
  `version` varchar(256) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table w_time_jobs
# ------------------------------------------------------------

CREATE TABLE `w_time_jobs` (
  `owner` varchar(256) DEFAULT NULL,
  `cron` varchar(1024) DEFAULT NULL,
  `id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table w_time_jobs_status
# ------------------------------------------------------------

CREATE TABLE `w_time_jobs_status` (
  `in_degree` int(11) DEFAULT NULL,
  `out_degree` int(11) DEFAULT NULL,
  `is_executed` text,
  `is_success` text,
  `msg` text,
  `cron` text,
  `owner` varchar(256) DEFAULT NULL,
  `id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
