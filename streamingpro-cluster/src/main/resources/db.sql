CREATE TABLE backend
(
  id                   int(11) NOT NULL AUTO_INCREMENT,
  name                 varchar(255) DEFAULT NULL,
  url                  text,
  tag                  text,
  ecs_resource_pool_id int(11),
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE ecs_resource_pool
(
  id           int(11) NOT NULL AUTO_INCREMENT,
  name         varchar(255) DEFAULT NULL,
  login_user   varchar(255),
  execute_user varchar(255),
  ip           text,
  host         text,
  key_path     text,
  spark_home   text,
  mlsql_home   text,
  mlsql_config text,
  in_use       varchar(255),
  tag          text,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE elastic_monitor
(
  id                int(11) NOT NULL AUTO_INCREMENT,
  name              varchar(255) DEFAULT NULL,
  min_instances     int(11),
  max_instances     int(11),
  allocate_type     varchar(255),
  allocate_strategy varchar(255),
  tag               text,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;