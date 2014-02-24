#!/bin/sh

USER='root'
HOST='localhost'

mysql -u$USER -h$HOST <<EOL

CREATE DATABASE snapshot_db;
USE snapshot_db;

DROP TABLE IF EXISTS snapshot_table;
CREATE TABLE snapshot_table (
  id int(11) NOT NULL,
  name varchar(255) DEFAULT NULL,
  snapshot_id int(11) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
EOL

for i in `seq 1 10000`
do
  GROUP=`expr $RANDOM % 2`
  INSERT="INSERT INTO snapshot_table (id, name, snapshot_id) values ($i, \"name_$i\", $GROUP);"
  mysql -u$USER -h$HOST snapshot_db -e "$INSERT"
done

