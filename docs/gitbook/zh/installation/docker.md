## 命令
在终端执行如下指令：

```shell
bash <(curl http://download.mlsql.tech/scripts/run-all.sh)
```
注意事项:

0. 用户需要确保在操作系统为Linux（Mac 也是Ok的），有docker环境即可。
0. 请确保执行的电脑 9002,9003,8080三个端口没有占用
1.  脚本执行，可以看到MySQL连接错误。这是脚本在等待MySQL启动后可用。不是错误，请放心。
2. 如果docker镜像拉去缓慢，可以设置阿里云镜像。具体操作如下：

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

## 登录注册

访问 http://127.0.0.1:9002 :


![image.png](http://docs.mlsql.tech/upload_images/1063603-5dbdcd1e735e9681.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

因为在MLSQL Console中，任何用户在使用控制台的时候，必须先自己创建team/role 或者加入到别人的team/role之后才能连接到真正的后端执行操作。所以这个时候你需要自己到Team标签页进行设置。

![image.png](http://docs.mlsql.tech/upload_images/1063603-164c90c01d45a255.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

第一次进来，会提示你没有配置后端。第一步先创建一个团队。

![image.png](http://docs.mlsql.tech/upload_images/1063603-b3bf4a420a3c854f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

完成会进行相应提示：

![image.png](http://docs.mlsql.tech/upload_images/1063603-f03b4786fc18fd28.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

这个时候你需要切换下标签，比如切到demo或者team里（或者刷新），不然后面有部分板块没有得到及时更新。在team标签我们继续往下拉找到创建Role板块：

![image.png](http://docs.mlsql.tech/upload_images/1063603-f713d028577a6703.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

接着把自己放到admin角色里：

![image.png](http://docs.mlsql.tech/upload_images/1063603-f5067df22572e2e4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

现在，我么只要给角色添加一个后端即可（点击左侧Cluster标签页）：

![image.png](http://docs.mlsql.tech/upload_images/1063603-c5ca9f01a051aa6b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Server地址一定要和我一样。

因为一个用户可能有多个角色，所以可能有多组服务器，我们需要固定一组，通过左侧面板可以完成最后一步设置：

![image.png](http://docs.mlsql.tech/upload_images/1063603-7ee9b2b8d5e7b753.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

大部分输入框都有提示，大家不用担心。

现在，可以开始试用Console了。

![image.png](http://docs.mlsql.tech/upload_images/1063603-a52f103ec5c8d0b7.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

拖拽蓝色框到编辑区里，然后点击运行，第一次会有点慢。

![image.png](http://docs.mlsql.tech/upload_images/1063603-2be788e177ffccb5.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

搞定。

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