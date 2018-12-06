set streamName="streamExample";

load kafka9.`-` options `kafka.bootstrap.servers`="127.0.0.1:9092"
and `topics`="testM"
as newkafkatable1;

select "abc" as col1,decodeKafka(value) as col2 from newkafkatable1
as table21;

save append table21
as carbondata.`-`
options mode="append"
and duration="10"
and dbName="default"
and tableName="carbon_table2"
and `carbon.stream.parser`="org.apache.carbondata.streaming.parser.RowStreamParserImp"
and checkpointLocation="/data/carbon/store/default/carbon_table2/.streaming/checkpoint";