# 环境依赖

在运行MLSQL Driver(Executor节点可选)的节点上，需要有Python环境。如果你使用yarn,推荐使用Conda管理Python环境。如果你使用K8s,可以直接使用镜像管理。

Python环境最好是3.6版本，并且请安装如下依赖：

```
pip install Cython
pip install pyarrow==0.10.0
pip install ray==0.8.0
pip install aiohttp psutil setproctitle grpcio pandas xlsxwriter
pip install watchdog requests click uuid sfcli  pyjava
```

如果你要使用Ray做计算，请确保Driver节点（Executor节点可选）按如下方式启动ray worker:

```
ray start --address=<address> --num-cpus=0 --num-cpus=0
```

其中address地址为Ray集群地址，类似123.45.67.89:6379这样。同时我们将cpu,gpus等资源设置为0. 启动一个没有资源的ray worker ,是因为我们需要通过ray worker提交任务。

## 如何显示实时日志

MLSQL Console支持实时日志显示，

![](http://docs.mlsql.tech/upload_images/1cf48031-b8d4-4b33-b46f-628c1321045a.png)

为了支持上面这个功能，用户需要修改SPARK_HOME/conf下的log4j.properties文件。 复制黏贴如下范例：

[MLSQL log4j.properties](https://github.com/allwefantasy/mlsql/blob/master/streamingpro-mlsql/src/main/resources-online/log4j.properties)