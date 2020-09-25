#K8S部署

> 目前实测Spark 3.0 通过。建议使用3.0 进行K8s部署。

目前K8s部署有两种方式：

1. Spark官方的 [spark-submit方式](http://spark.apache.org/docs/latest/running-on-kubernetes.html)
2. Google 开发的[Operater方式](https://github.com/GoogleCloudPlatform/spark-on-k8s-operator)

两种方式的差别是，Google是裸露K8s的命令来进行部署的，而Spark官方则是通过Spark-Submit透明完成K8s部署的。另外，Google的目前比较适合作为一次性任务提交，Spark官方的更适合将Spark部署成服务。而MLSQL Engine主要以Service方式部署，所以我们推荐官方的部署模式。

在K8s部署MLSQL Engine的基本思路是,将spark-submit命令作为一个K8s Pod的启动命令，就像我们平时启动一个web服务一样。启动完成后，spark-submit会自动向K8s 申请executor Pods.

如果是手工操作，就是启动一个pod,然后在里面运行spark-submit命令，spark-submit会完成如下两件事：

1. 启动9003端口，也就是MLSQL Engine的HTTP端口,外部可以通过该端口访问。
2. 向K8s申请 Executor Pods

最后，我们将第一个Pod增加service以及ingress配置后就可以对外提供服务了。

## 配置文件示例

下面是Deployment 的template:

```yaml
 template:
    metadata:
      labels:
        app: spark-mlsql-3
    spec:
      containers:
      - name: spark-mlsql-3
        args:
          - >-
            echo "/opt/spark/bin/spark-submit --master k8s://https://xxx.xxx.xxx.xxx:xxxx 
            --deploy-mode client 
            --jars local:///tmp/streamingpro-mlsql-spark_3.0_2.12-1.7.0-SNAPSHOT.jar 
            --class streaming.core.StreamingApp 
            --conf spark.kubernetes.container.image=xxxxx/dev/spark-jdk-slim-14:v3.0.0 
            --conf spark.kubernetes.container.image.pullPolicy=Always 
            --conf spark.kubernetes.namespace=dev 
            --conf spark.kubernetes.authenticate.driver.serviceAccountName=xxxxx 
            --conf spark.kubernetes.container.image.pullSecrets=xxxxx 
            --conf spark.kubernetes.driver.request.cores=1 
            --conf spark.kubernetes.driver.limit.cores=1 
            --conf spark.kubernetes.executor.request.cores=1 
            --conf spark.kubernetes.executor.limit.cores=1 
            --conf spark.executor.instances=1 
            --conf spark.driver.host=$POD_IP 
            --conf spark.sql.cbo.enabled=true 
            --conf spark.sql.adaptive.enabled=true 
            --conf spark.sql.cbo.joinReorder.enabled=true 
            --conf spark.sql.cbo.planStats.enabled=true 
            --conf spark.sql.cbo.starSchemaDetection=true 
            --conf spark.driver.maxResultSize=2g 
            --conf spark.serializer=org.apache.spark.serializer.KryoSerializer 
            --conf spark.kryoserializer.buffer.max=200m 
            --conf "\"spark.executor.extraJavaOptions=-XX:+UnlockExperimentalVMOptions -XX:+UseZGC -XX:+UseContainerSupport  -Dio.netty.tryReflectionSetAccessible=true\""  
            --conf "\"spark.driver.extraJavaOptions=-XX:+UnlockExperimentalVMOptions -XX:+UseZGC -XX:+UseContainerSupport  -Dio.netty.tryReflectionSetAccessible=true\"" 
            local:///tmp/streamingpro-mlsql-spark_3.0_2.12-1.7.0-SNAPSHOT.jar 
            -streaming.name spark-mlsql-3.0 
            -streaming.rest true 
            -streaming.thrift false 
            -streaming.platform spark 
            -streaming.enableHiveSupport true 
            -streaming.spark.service true 
            -streaming.job.cancel true 
            -streaming.datalake.path /data/mlsql/datalake 
            -streaming.driver.port 9003" | bash
        command:
          - /bin/sh
          - '-c'
```

### 需要注意的配置点-POD_IP
spark.driver.host 需要配置成 $POD_IP,具体方式如下：

```yaml
        env:
          - name: POD_IP
            valueFrom:
              fieldRef:
                fieldPath: status.podIP
          - name: NODE_IP
            valueFrom:
              fieldRef:
                fieldPath: status.hostIP
```

### 需要注意的配置点-serviceAccountName

通常在K8s里需要配置`--conf spark.kubernetes.authenticate.driver.serviceAccountName=xxxx`, 目的是为了过K8s相关的权限。

### 需要注意的配置点-Java参数配置

如果你使用JDK8的镜像，那么需要移除`spark.executor.extraJavaOptions`,`spark.driver.extraJavaOptions`等两个配置选项。他们是为JDK14准备的。

### 如何获取配置中的镜像
其中，上面设计的镜像xxxxx/dev/spark-jdk-slim-14:v3.0.0可以使用官方提供的镜像工具来获得。具体方式如下(以Spark2.4.3版本为例)：

```shell
 cd $SPARK_HOME && 
     ./bin/docker-image-tool.sh -t v2.4.3 build
```     

上面执行完会打三个images：spark:v2.4.3 / spark-py:v2.4.3 / spark-r:v2.4.3

你如果只需要spark基本镜像包可执行：
    
```shell 
     cd $SPARK_HOME &&
     docker build -t spark:latest -f kubernetes/dockerfiles/spark/Dockerfile . 
```
### MLSQL Engine Jar包如何放到镜像里

可以把MLSQL Engine Jar包使用K8s的Volume功能挂载到pod里去。同理--jars 目录。Executor的Pod不需要挂载。

### Hadoop相关配置如何放到镜像里

同样可以使用K8s Volumn进行挂载，然后设置环境变量。也可以将相关配置放到SPARK_HOME/conf里之后再执行镜像构建命令。

###  Spark 3.0版本如何兼容hive 1.2

Spark3.0默认不兼容 hive 1.2 需要自己从Spark源码重新打包。譬如下面的编译参数就指定了hive-1.2的支持，然后再使用这个发行包构建镜像。

```
git checkout -b v3.0.0 v3.0.0
./dev/make-distribution.sh \
--name hadoop2.7 \
--tgz \
-Pyarn -Phadoop-2.7 -Phive-1.2 -Phive-thriftserver -Pkubernetes \
-DskipTests
```

### LZO/Snappy的问题

LZO的jar包直接丢SPARK_HOME/jars里即可。

Snappy需要通过--conf java参数指定：

```
--conf spark.executor.extraJavaOptions=-Djava.library.path=/opt/.../lib/native/
```

请确保所有的镜像里都有配置的路径。


## 如何配置Service实现外部访问

我们可以通过配置service/ingress来实现外部访问Engine.

首先是service.yaml示例：

```yaml
apiVersion: v1
kind: Service
metadata:
  annotations: {}
  name: spark-mlsql
  namespace: dev
spec:
  ports:
    - name: mlsql
      port: 9003
      targetPort: 9003
    - name: spark
      port: 4040
      targetPort: 4040
  selector:
    app: spark-mlsql-3 
```


接着ingress.yaml示例：

```yaml
apiVersion: extensions/v1beta1
kind: Ingress
metadata:
  name: spark-mlsql-ingress
  namespace: dev
spec:
  rules:
    - host: xxxxxxxxx
      http:
        paths:
        - path: /
          backend:
            serviceName: spark-mlsql 
            servicePort: 9003
    - host: xxxxxxxx
      http:
        paths:
        - path: /
          backend:
            serviceName: spark-mlsql
            servicePort: 4040
 
```


