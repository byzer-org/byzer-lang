# How to run in docker



Run this command in your terminal：

```shell
bash <(curl http://download.mlsql.tech/scripts/run-all.sh)
```

Notices:

0. Your system should be Linux/Mac OS, and have docker installed. 
0. 9002,9003,8080,3306 should be not taken by other programs.
1. If there are some MySQL errors, ignore them, the script is checking when the mysql is ready.


Using `docker ps` to check the result:

![image.png](http://docs.mlsql.tech/upload_images/1063603-004da41021835b54.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

All docker containers are up.

## Register/Login

Visit  `http://127.0.0.1:9002`(only chrome browser is tested):


![image.png](http://docs.mlsql.tech/upload_images/1063603-5dbdcd1e735e9681.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Join a team/role is required to use the console. You can create your own team/role.

![image.png](http://docs.mlsql.tech/upload_images/1063603-164c90c01d45a255.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

First, create a team.

![image.png](http://docs.mlsql.tech/upload_images/1063603-b3bf4a420a3c854f.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

And the console will show you the result.

![image.png](http://docs.mlsql.tech/upload_images/1063603-f03b4786fc18fd28.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

You need to refresh manually, otherwize some panel may not been refreshed in time.
Pull the page and find the Role panel:


![image.png](http://docs.mlsql.tech/upload_images/1063603-f713d028577a6703.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Put you in the admin role:

![image.png](http://docs.mlsql.tech/upload_images/1063603-f5067df22572e2e4.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Click the Cluster menu,add the backend manually:

![image.png](http://docs.mlsql.tech/upload_images/1063603-c5ca9f01a051aa6b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

The server name should like above exactly.

Cause one user can have many roles, you should bind a default role:

![image.png](http://docs.mlsql.tech/upload_images/1063603-7ee9b2b8d5e7b753.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Since every input have the auto-suggession, do not be worry.

Now, feel free to use the console:

![image.png](http://docs.mlsql.tech/upload_images/1063603-a52f103ec5c8d0b7.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Drag the blue block to the eidtor, the first time to do this, it's a little slow:


![image.png](http://docs.mlsql.tech/upload_images/1063603-2be788e177ffccb5.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

Done.

## Notice
We disable the table auth by default since when this is opened which will make you not easy to use.
You can enable it when you start MLSQL console Docker container with adding "-e ENABLE_AUTH_CENTER=true".


```shell
#!/usr/bin/env bash

java -cp .:${MLSQL_CONSOLE_JAR} tech.mlsql.MLSQLConsole \
-mlsql_cluster_url ${MLSQL_CLUSTER_URL} \
-my_url ${MY_URL} \
-user_home ${USER_HOME} \
-enable_auth_center ${ENABLE_AUTH_CENTER:-false} \
-config ${MLSQL_CONSOLE_CONFIG_FILE}
```
