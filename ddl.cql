CREATE KEYSPACE distribution WITH REPLICATION={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES=true;
CREATE TABLE et_totals (key text, count counter, PRIMARY KEY(key));
