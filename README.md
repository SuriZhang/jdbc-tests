# jdbc-tests
Test generator for JDBC drivers

# To run tests on postgres
run script: demo/src/test/java/demo/data/postgres/bin/postgres-server 

# To run tests on duckdb
No special handling required, data will be stored under `demo/src/test/java/demo/data/duckdb/tmp.db`. This db file will be created automatically.
