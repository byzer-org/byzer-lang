# Stream schema infer 

If data in Kafka format is json, then you can use `kafkaTool` to sample the data and 
infer the schema.


```sql
-- the stream name, should be uniq.
set streamName="kafkaStreamExample";

-- sample 2 records and infer the schema from kafka server "127.0.0.1:9092" and the
-- topic is wow.
-- Make sure this statement is placed before the load statement.
!kafkaTool registerSchema 2 records from "127.0.0.1:9092" wow;

-- convert table as stream source
load kafka.`wow` options 
kafka.bootstrap.servers="127.0.0.1:9092"
as newkafkatable1;

-- aggregation 
select *  from newkafkatable1
as table21;

-- output the the result to console.
save append table21  
as rate.`/tmp/delta/wow-0` 
options mode="Append"
and duration="5"
and checkpointLocation="/tmp/s-cpl4";

```