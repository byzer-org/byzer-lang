# 使用Docker

MLSQL-Engine,MLSQL-Cluster,MLSQL-Console目前都提供了Docker镜像。MLSQL同时提供了一套脚本方便我们启动一个包含这三个组件
系统。

## Docker使用步骤

1. 克隆 mlsql console项目,主要是为了获取脚本：

```
git clone https://github.com/allwefantasy/mlsql-api-console .
cd mlsql-api-console/dev
```

2. 配置网络

```
./run-network.sh
```

3. 启动DB并且创建数据库和表

```
./run-db.sh
```

启动会提示连接报错，千万别慌张，这个是我们在检查MySQL何时Ready以方便写入数据。

4. 启动MLSQL Engine

```
./run-single-mlsql-server.sh
```

5. 启动MLSQL Console

```
./run-mlsql-console.sh
```

6. 在Chrome访问url 网址 http://127.0.0.1:9002，看到登录界面

![](https://upload-images.jianshu.io/upload_images/1063603-84958d2a80b08f8c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

点击右上方注册，输入邮箱和密码

![image.png](https://upload-images.jianshu.io/upload_images/1063603-c401ff6f6fec9bc3.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

注册成功会自动进入控制台界面。

如果希望直接使用MLSQL-Engine,则访问http://127.0.0.1:9003即可。

## 如何使用启动mlsql-cluster 

进入该项目，[streamingpro-cluster/dev](https://github.com/allwefantasy/streamingpro/tree/master/streamingpro-cluster/dev) 

分别运行 run-db.sh /run-mlsql-cluster.sh 即可。 不过目前cluster 还没有纳入console管理，全部通过API操作，大家可以先不管。

