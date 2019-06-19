# Binlog Source

MLSQL provides MySQL Binlog datasource. This means you can connect MySQL
directly to get the binlog and incrementally sync MySQL table to delta table.

# Usage

```sql
set streamName="binlog";

load binlog.`` where 
host="127.0.0.1"
and port="3306"
and userName="xxx"
and password="xxxx"
and bingLogNamePrefix="mysql-bin"
and startingOffsets="40000000000004"
and databaseNamePattern="mlsql_console"
and tableNamePattern="script_file"
as table1;

save append table1  
as binlogRate.`/tmp/binlog1/{db}/{table}` 
options mode="Append"
and idCols="id"
and duration="5"
and checkpointLocation="/tmp/cpl-binlog";
```

You can use databaseNamePattern,tableNamePattern to filter the table you want to sync,
They accept regex.

When you recovery from `checkpointLocation`, then the offset read from ck is prior then  `startingOffsets` you  specified.
Notice that you can also use kafka command to get the latest committed offset. 

```sql
!kafka  streamOffset /tmp/cpl-binlog;  
```

If you want to make the startingOffsets take effect, then you should set a new `checkpointLocation`.

binlogRate will execute upsert/delete on records according to the idCols. If you just want to save the original binlog, 
use rate instead of binlogRate.

## Limitation for this version
0. Enable binlog and set `binlog_format=Row` in my.cnf
1. Only update/delete/insert are supported.
2. Once the Stream fails, At least once guarantee
3. The table loaded as binlog format only can be saved by one `save` statement. 
4. Once the save task fails and retry, it may lost data.
5. When the computation/sink is too slow, the buffer which we keep raw binlog in executor, is greater then maxBinlogQueueSize,
   then we pause the MySQL binlog consumer, and stop put more data into buffer. When the buffer is maxBinlogQueueSize/2 ,
   resume the binlog consumer
   
The 3,4 limitation is caused by we consume MySQL binlog and put them in executor queue(memory), and the other executor will
take the data from queue, and the queue will not keep the data have been taken. So once the take/save task fails and try again, they will
get nothing.       

             
 