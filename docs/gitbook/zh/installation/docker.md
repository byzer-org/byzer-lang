## 命令

在终端执行如下指令：

```shell
bash <(curl http://download.mlsql.tech/scripts/run-all.sh)
```

或者

```
wget http://download.mlsql.tech/scripts/run-all.sh
chmod u+x run-all.sh
./run-all.sh
```

注意事项:

0. 用户需要确保在操作系统为Linux（Mac 也是Ok的），有docker环境即可。
1. 请确保执行的电脑 9002,9003,8080,3306四个端口没有占用
2. 脚本执行，可以看到MySQL连接错误。这是脚本在检测MySQL启动后是否可用。不是错误，请放心。


如果docker镜像拉去缓慢，可以设置阿里云镜像。具体操作如下：

```shell
mkdir -p /etc/docker

## 登录后阿里开发者帐户后，[https://cr.console.aliyun.com/#/accelerator](https://link.jianshu.com?t=https%3A%2F%2Fcr.console.aliyun.com%2F%23%2Faccelerator) 中查看你的您的专属加速器地址

tee /etc/docker/daemon.json <<-'EOF'
{
  "registry-mirrors": ["https://[这里的加速地址要替换成你自己的，到阿里云控制台获取].mirror.aliyuncs.com"]
}
EOF

systemctl daemon-reload
systemctl restart docker
```

接着 docker ps:

![image.png](http://docs.mlsql.tech/upload_images/1063603-004da41021835b54.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

可以看到相关容器都启动了。

## 快速配置向导

访问 http://127.0.0.1:9002 进行注册。只支持Gmail邮箱。

![image.png](http://docs.mlsql.tech/upload_images/1063603-5dbdcd1e735e9681.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

进入后看到如下界面,点击Team:

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095256.png)

可以见到一个快速设置页：

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095357.png)

随便填入一个team名称，点击下一步：


![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095440.png)

选中刚才建立的team,然后设置一个角色，下一步

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095542.png)

把自己邀请到team1/admin里，下一步：

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095644.png)

开始给team和role添加后端engine,这里url 一定要 填写： mlsql-server:9003. 因为我使用了docker网络。点击下一步：

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095834.png)

将刚才的添加的engine设置为默认的engine,点击下一步：

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095955.png)

完成。

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-100026.png)

这个时候返回主界面，然后执行一个命令看是否都联通了(第一次运行会比较久)：

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-100144.png)

大工告成。如果你想要看更多示例，请登录 try.mlsql.tech,然后用一下用户名登录查看：

```
account:  demo@gmail.com
password: 123456
```


## 结束语
在docker里我们默认关闭了表权限校验，否则使用时，使用jdbc等各种数据的时候，都需要添加权限，比较繁琐。打开的方式是在mlsql-console 进行docker run的时候 加上-e ENABLE_AUTH_CENTER=true 即可。参看启动脚本 start.sh:

```shell
#!/usr/bin/env bash

java -cp .:${MLSQL_CONSOLE_JAR} tech.mlsql.MLSQLConsole \
-mlsql_cluster_url ${MLSQL_CLUSTER_URL} \
-my_url ${MY_URL} \
-user_home ${USER_HOME} \
-enable_auth_center ${ENABLE_AUTH_CENTER:-false} \
-config ${MLSQL_CONSOLE_CONFIG_FILE}
```
## ChatRoom
![image](http://upload-images.jianshu.io/upload_images/1063603-f32dd474770fe70d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/160) 
![image](http://upload-images.jianshu.io/upload_images/1063603-27e80786d337fc7d.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/160)