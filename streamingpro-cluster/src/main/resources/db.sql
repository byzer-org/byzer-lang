# ************************************************************
# Sequel Pro SQL dump
# Version 4096
#
# http://www.sequelpro.com/
# http://code.google.com/p/sequel-pro/
#
# Host: 127.0.0.1 (MySQL 5.7.20)
# Database: streamingpro_cluster
# Generation Time: 2019-05-27 06:02:00 +0000
# ************************************************************


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;


# Dump of table backend
# ------------------------------------------------------------

DROP TABLE IF EXISTS `backend`;

CREATE TABLE `backend` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `url` text,
  `tag` text,
  `ecs_resource_pool_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table ecs_resource_pool
# ------------------------------------------------------------

DROP TABLE IF EXISTS `ecs_resource_pool`;

CREATE TABLE `ecs_resource_pool` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `login_user` varchar(255) DEFAULT NULL,
  `execute_user` varchar(255) DEFAULT NULL,
  `ip` text,
  `host` text,
  `key_path` text,
  `spark_home` text,
  `mlsql_home` text,
  `mlsql_config` text,
  `in_use` varchar(255) DEFAULT NULL,
  `tag` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;



# Dump of table elastic_monitor
# ------------------------------------------------------------

DROP TABLE IF EXISTS `elastic_monitor`;

CREATE TABLE `elastic_monitor` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `min_instances` int(11) DEFAULT NULL,
  `max_instances` int(11) DEFAULT NULL,
  `allocate_type` varchar(255) DEFAULT NULL,
  `allocate_strategy` varchar(255) DEFAULT NULL,
  `tag` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;




/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
