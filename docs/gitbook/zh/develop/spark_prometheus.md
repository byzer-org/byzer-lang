1、准备（本测试基于 `linux centos6.5`）

```
graphite_exporter下载地址
https://github.com/prometheus/graphite_exporter/releases

prometheus下载地址
https://prometheus.io/download/

grafana下载地址
https://grafana.com/grafana/download

通过wget下载
```

2、`spark`没有直接写入`Prometheus`的插件，但是有`Graphite Sink`插件，`prometheus`有`graphite_exporter`插件，可以将`Graphite metrics`写入`Prometheus`。


3、原生的`Graphite`数据可以通过映射文件转化为有`label`维度的`Prometheus`数据。
例如：

```
mappings:
- match: '*.*.jvm.*.*'
  name: jvm_memory_usage
  labels:
    application: $1
    executor_id: $2
    mem_type: $3
    qty: $4
```

上述文件会将数据转化成`metric name`为`jvm_memory_usage`，`label`为`application`，`executor_id`，`mem_type`，`qty` 的格式。

```
application_1574214992329_0017_1_jvm_heap_usage -> jvm_memory_usage{application="application_1574214992329_0017",executor_id="driver",mem_type="heap",qty="usage"}
```

`graphite_mapping`文件如下

```
mappings:
- match: '*.*.executor.filesystem.*.*'
  name: filesystem_usage
  labels:
    application: $1
    executor_id: $2
    fs_type: $3
    qty: $4

- match: '*.*.jvm.*.*'
  name: jvm_memory_usage
  labels:
    application: $1
    executor_id: $2
    mem_type: $3
    qty: $4

- match: '*.*.executor.jvmGCTime.count'
  name: jvm_gcTime_count
  labels:
    application: $1
    executor_id: $2

- match: '*.*.jvm.pools.*.*'
  name: jvm_memory_pools
  labels:
    application: $1
    executor_id: $2
    mem_type: $3
    qty: $4

- match: '*.*.executor.threadpool.*'
  name: executor_tasks
  labels:
    application: $1
    executor_id: $2
    qty: $3

- match: '*.*.BlockManager.*.*'
  name: block_manager
  labels:
    application: $1
    executor_id: $2
    type: $3
    qty: $4

- match: DAGScheduler.*.*
  name: DAG_scheduler
  labels:
    type: $1
    qty: $2
```

4、`prometheus`安装

```
tar -zxvf prometheus-2.8.0.linux-amd64.tar.gz
mv /usr/local/prometheus-x.x.x.linux-amd64 /usr/local/prometheus
	
查看Prometheus版本
cd /usr/local/prometheus
./prometheus --version
	
备注：默认的配置文件prometheus.yml，默认启动后的端口为9090
```

5、配置 Prometheus 从 graphite_exporter 获取数据，修改prometheus.yml

```
scrape_configs:
  - job_name: 'spark'
    static_configs:
    - targets: ['localhost:9108']
```
9108为graphite_exporter服务端口

6、配置spark conf下的metrics.properties，也可以用指定metrics.properties模式，百度一下便知。
	
```
*.sink.graphite.class=org.apache.spark.metrics.sink.GraphiteSink
*.sink.graphite.host=localhost
*.sink.graphite.port=9019
*.sink.graphite.period=5
*.sink.graphite.unit=seconds
	
driver.source.jvm.class=org.apache.spark.metrics.source.JvmSource
executor.source.jvm.class=org.apache.spark.metrics.source.JvmSource
```
	
9019为graphite_exporter接受数据端口


7、安装`granafa`和`graphite_exporter`

```
解压即可，启动文件在bin目录
```


8、启动`graphite_exporter`时加载配置文件，服务端口默认为`9108`，接受数据端口默认为`9109`

```
nohup ./graphite_exporter --graphite.mapping-config=graphite_mapping &
```

9、启动`Prometheus`

`./prometheus &`

10、启动`grafana`

`nohup ./grafana-server start &`
	
根据自己的需要，配置监控指标

11、启动mlsql


12、补充
	
1. 查看`graphite_exporter`的`metrics`
	
	`http://127.0.0.1:9108/metrics`
	
2. `Prometheus web`地址
	
	`http://127.0.0.1:9090`
	
	可以执行如下查询：
	
	`filesystem_usage{qty="read_bytes", fs_type="hdfs",  application="application_1574214992329_0031"}`
	
3. `grafana web`地址
	
	`http://127.0.0.1:3000`









