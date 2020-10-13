## 使用Docker安装体验

 【文档更新日志：2020-04-19】

> Note: 本文档适用于MLSQL Engine 1.6.0-SNAPSHOT/1.6.0 及以上版本。  
> 对应的Spark版本可支持2.4.5
>


## 注意

因为我们将镜像发布在了Docker Hub,不同网络情况下不是很稳定。
如果docker镜像拉取缓慢，可以设置阿里云镜像。首先去阿里云开通镜像服务，然后进入
https://cr.console.aliyun.com/cn-hangzhou/instances/mirrors 页面，里面会教你做镜像加速。

具体操作如下：

```shell
mkdir -p /etc/docker
## 登录后阿里开发者帐户后，
## 在https://cr.console.aliyun.com/cn-hangzhou/instances/mirrors 中查看你的您的专属加速器地址

tee /etc/docker/daemon.json <<-'EOF'
{
  "registry-mirrors": ["https://[这里的加速地址要替换成你自己的，到阿里云控制台获取].mirror.aliyuncs.com"]
}
EOF

systemctl daemon-reload
systemctl restart docker
```

执行时间取决于你的下载速度。

## 最快方式

```
bash <(curl http://download.mlsql.tech/scripts/run-all.sh)
```

## Python库安装

如果需要安装python的一些库，按如下方式操作。

1. 进入交互式shell

```
docker exec  -it mlsql-server bash
```

切入conda环境：

```
source activate dev
```

pip 安装然后退出即可：

```
pip install 库名
```

## 手动搭建

创建网络:

```
docker network rm  mlsql-network
docker network create mlsql-network
```

启动MySQL容器：

```
docker run --name mlsql-db -p 3306:3306 \
--network mlsql-network \
-e MYSQL_ROOT_PASSWORD=mlsql \
-d techmlsql/mlsql-db:1.6.0-SNAPSHOT
```

启动MLSQL Engine:

```
docker run --name mlsql-server -d \
--network mlsql-network \
-p 9003:9003 \
techmlsql/mlsql:spark_2.4-1.6.0-SNAPSHOT
```

启动 MLSQL Console:

```
docker run --name mlsql-console \
--network mlsql-network \
-p 9002:9002 \
-e MLSQL_ENGINE_URL=http://mlsql-server:9003 \
-e MY_URL=http://mlsql-console:9002 \
-e USER_HOME=/tmp/users \
-e MYSQL_HOST=mlsql-db \
-d \
techmlsql/mlsql-console:1.6.0-SNAPSHOT
```








