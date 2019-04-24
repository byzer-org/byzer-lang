#MLSQL Engine Start Configruatioin

If you have started the MLSQL Stack, just use command like this to get the parameters:

```sql
load mlsqlConf.`` as output;
```

下面是常常见参数说明：


|Parameter | Description |Default|
|---- | --- |---- |
|streaming.name | the service name in spark ui | |
|streaming.ps.cluster.enable | Used in cluster mode，Python ET needs this enabled.   |false|
|streaming.ps.local.enable | Use in local mode, Python ET needs this enabled.  |true|
|streaming.bigdl.enable | enable bigdl support  |true|
|streaming.udf.clzznames | the full name of you udf classes.  ||
|streaming.master | equal to --master  ||
|streaming.platform | for now only support: spark  ||
|streaming.deploy.rest.api | When deploy MLSQL as predict service(Local) enable it will speed the performance |false|
|streaming.enableHiveSupport | enable hive  |false|
|streaming.enableCarbonDataSupport| enable carbodnata  |false|
|streaming.carbondata.store | carbondata storage path  ||
|streaming.carbondata.meta | carbondata meta path  ||
|streaming.rest | enable rest  |false|
|streaming.driver.port | rest port  ||
|streaming.spark.service | as service  |false|
|streaming.zk.servers | the zk address so the driver can register itself  ||
|streaming.zk.conf_root_dir | the zk path where the driver can register itself  ||

## Notices

1. When in yarn-cluster mode, the address of driver is undefined, we can configure  `-streaming.zk.servers`
and `-streaming.zk.conf_root_dir` when startup，MLSQL will register the address and port to zk.

2. When treat MLSQL as a Rest Service, you should enable these items: `streaming.rest`,`streaming.driver.port`,`streaming.zk.servers`
三个参数。

3. `streaming.platform` must be configured and must be`spark`. 