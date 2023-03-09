# jdbc-tests
Test generator for JDBC drivers

# To run tests on postgres
run script to start a test container: 'demo/src/test/java/demo/data/postgres/bin/postgres-server'  
the entire directory was copied from https://github.com/pgjdbc/pgjdbc/tree/master/docker

# To run tests on duckdb
No special handling required, data will be stored under `demo/src/test/java/demo/data/duckdb/tmp.db`. This db file will be created automatically.

# To run tests on mysql
run docker container: `docker run -itd --name mysql-test -p 3306:3306 -e MYSQL_ROOT_PASSWORD=root mysql`  
access container mysql db: `mysql -h 127.0.0.1 -P 3306 -u root -p`  
first time setup, run the following SQL queries in mysql:  
```
CREATE DATABASE test;
CREATE USER 'test'@'%' IDENTIFIED BY 'test';
GRANT CREATE, ALTER, DROP, INSERT, UPDATE, DELETE, SELECT, REFERENCES, RELOAD on *.* TO 'test'@'%' WITH GRANT OPTION;
```
after first time, just restart container: `docker restart mysql-test`
