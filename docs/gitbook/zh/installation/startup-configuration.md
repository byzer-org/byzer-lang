#启动参数说明

## MLSQL-Engine

所有启动参数，可以使用如下指令进行查看：

```sql
load mlsqlConf.`` as output;
```

下面是常常见参数说明：


|参数 | 说明 |默认值|
|---- | --- |---- |
|streaming.name | 显示在spark ui上的名字 | |
|streaming.ps.cluster.enable | 集群模式下，开启driver和worker单独通讯模式。使用Python相关模块需要开启该功能  |false|
|streaming.ps.local.enable | local模式下，开启driver和worker单独通讯模式。使用Python相关模块需要开启该功能  |true|
|streaming.bigdl.enable | 是否开启内置深度学习支持  |true|
|streaming.udf.clzznames | 自定义udf函数包  ||
|streaming.master | 等价于--master  ||
|streaming.platform | 目前只支持spark  ||
|streaming.deploy.rest.api | 用Local模式部署成API时，请开启该选项，此参数会导致hive失效，默认sparkcontext会被替换  |false|
|streaming.enableHiveSupport | 开启Hive支持  |false|
|streaming.enableCarbonDataSupport| 开启carbondata支持  |false|
|streaming.carbondata.store | carbondata 存储路径  ||
|streaming.carbondata.meta | carbondata元信息路径  ||
|streaming.rest | 是否开启Rest支持  |false|
|streaming.driver.port | 开启Rest支持后的接口  ||
|streaming.spark.service | 是否作为常驻程序  |false|
|streaming.zk.servers | zk服务器地址  ||
|streaming.zk.conf_root_dir | 把当前driver 地址和端口注册到该ZK路径下  ||

## 常见问题

1. 如果使用yarn-cluster模式，这个时候driver地址是变化的，我们可以通过配置`streaming.zk.servers`
以及`streaming.zk.conf_root_dir`，MLSQL 会自动将driver的ip和端口注册到该zk上。应用可以通过
zk得到通知。

2. 我们将MLSQL-Engine作为常驻服务使用。需要开启 `streaming.rest`,`streaming.driver.port`,`streaming.zk.servers`
三个参数。

3. `streaming.platform`必须配置，而且只能是`spark`. 


