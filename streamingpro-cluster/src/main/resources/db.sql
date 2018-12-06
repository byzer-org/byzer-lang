CREATE TABLE backend
(
  id   int(11) NOT NULL AUTO_INCREMENT,
  name varchar(255) DEFAULT NULL,
  url  text,
  tag  text,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;