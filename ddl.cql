CREATE KEYSPACE tick WITH REPLICATION={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES=true;
CREATE TABLE distribution (key text, count counter, PRIMARY KEY(key));
CREATE TABLE total_events (type text, count counter, PRIMARY KEY(type));
CREATE TABLE dist_over_time ( attr_name text, attr_value text,  event_time timestamp, count counter,  PRIMARY KEY(attr_name, attr_value, event_time));
