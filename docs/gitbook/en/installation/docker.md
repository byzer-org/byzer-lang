# How to run in docker


Run the following in your terminal：

```shell
bash <(curl http://download.mlsql.tech/scripts/run-all.sh)
```

or in case it just does not work, try this way:

```
wget http://download.mlsql.tech/scripts/run-all.sh
chmod u+x run-all.sh
./run-all.sh
```

Notices:

0. macOs,Linux with docker pre installed。
1. Make sure 9002,9003,8080,3306 are not taken.
2. The script will check MySQL is ready. Please ignore the Mysql error message.

docker ps:

![image.png](http://docs.mlsql.tech/upload_images/1063603-004da41021835b54.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

All related containers are started.

## Quick configuration tutorial

Register in http://127.0.0.1:9002. Only Gmail supported.

![image.png](http://docs.mlsql.tech/upload_images/1063603-5dbdcd1e735e9681.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

The main page will show after you registry. Click the Team tab.

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095256.png)

You will see a quick configuration navigator:

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095357.png)

Type any team name you want：


![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095440.png) 

Select the team name you created in preview step, and create a new role fort it:

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095542.png)

Invite yourself to team1/admin, click next step:

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095644.png)

Add MLSQL engine for you role. Please make sure the url must be the same with me： mlsql-server:9003. 
click next step：

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095834.png)

Set the engine as the default engine for your role：

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-095955.png)

Done!

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-100026.png)

Return to the main page and try to run show jobs command. It will take a while 
since it's the first time to execute command.

![image.png](http://docs.mlsql.tech/upload_images/WX20190807-100144.png)

More mlsql code example，please login in try.mlsql.tech with the following user/password:

```
account:  demo@gmail.com
password: 123456
```


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
