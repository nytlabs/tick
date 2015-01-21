CREATE KEYSPACE tick WITH REPLICATION={ 'class' : 'SimpleStrategy', 'replication_factor' : 1 } AND DURABLE_WRITES=true;
-- CREATE TABLE distribution (key text, count counter, PRIMARY KEY(key));
CREATE TABLE distribution (stream text, key text, count counter, typ_bool_count counter, typ_str_count counter, typ_num_count counter, PRIMARY KEY(key));
CREATE TABLE total_events (stream text, count counter, PRIMARY KEY(stream));
CREATE TABLE dist_over_time (stream text, attr_name text, attr_value text,  event_time timestamp, count counter,  PRIMARY KEY(stream, attr_name, attr_value, event_time));
