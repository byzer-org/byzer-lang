# 权限和用户管理

MLSQL Console在设计时，用户是属于自组织的，没有最高权限用户。
这意味着，任何用户注册后，都可以建立自己的组织（Team）,角色（Role）,集群（Cluster）以及通过Role对每张表的权限控制。

值得注意的是，虽然MLSQL Engine实现了列级别权限控制，但是到现阶段（1.4.0）,MLSQL Console还不知道到列级别权限的设置。

我们先简要介绍下上面几个词汇的意义。

1. Team     组织
2. Role     角色
3. User     在MLSQL Console中存在的用户
4. Backend  主要是指MLSQL Engine实例
5. Table    表，权限实体

在MLSQL中，有如下几个约束：

1. Backend(MLSQL Engine实例) 被绑定在一个或者多个Role上。完成一次绑定，该实例会自动生成一个标签，格式为：team_role
2. User也需要绑定到一个或者多个Role上，一旦完成一次绑定，也会产生一个标签，格式为team_role.
3. 具有任意一个相同标签的User可以访问对应的Cluster
4. 用户同一时刻只能指定一个标签的Backend进行访问。也就是设置default backend tag.

这就意味着，如果一个用户想要能够正常使用MLSQL Console,他需要至少进行如下几个动作：

1. 创建Team/Role
2. 将自己加进自己创建的Team/Role
3. 添加Backend,也就是添加MLSQL Engine实例
4. 将Backend添加到Team/Role上
5. 设置对应某一个team_role为自己默认的backend tag
6. 用户可以在首页执行MLSQL 语句

  
如果用户仅仅是自己使用，那我们我们在Team/Setup提供了一个快捷向导，具体可参看[使用Docker安装体验](http://docs.mlsql.tech/zh/installation/docker.html)中的
快速配置向导部分。如果你不是使用Docker,唯一需要注意的是，在Create Backend部分填写实际的域名和端口。


## 创建新团队

点击导航栏的Team标签，之后点击右侧边栏栏的Team标签，红色框部分即为创建新Team的面板

![](http://docs.mlsql.tech/upload_images/WX20190819-182306.png)


创建完成后，即可在[Team belongs to you] 看到新创建的Team:


![](http://docs.mlsql.tech/upload_images/WX20190819-182602.png)

## 邀请会员进入你的团队

![](http://docs.mlsql.tech/upload_images/WX20190819-182747.png)

如果你发现下拉款没有你新建的组或者角色或者其他的时候，尝试点击Setup然后再点击回来（触发刷新）。

比如当前jack2里只有我自己：

![](http://docs.mlsql.tech/upload_images/WX20190819-182959.png)

现在我点击Create按钮后，jack2里就有新用户了。如果该用户不存在，上面会弹出提示说用户不存在。请仔细检查。

![](http://docs.mlsql.tech/upload_images/WX20190819-183109.png)

邀请后，被邀请用户需要确认，该用户可以到 [Team inviting you] 面板中点击拒绝或者同意。

## 给Team管理角色

![](http://docs.mlsql.tech/upload_images/WX20190819-183340.png)

## 将邀请的用户添加到特定角色中
  
![](http://docs.mlsql.tech/upload_images/WX20190819-183340.png)

## 添加MLSQL Engine实例

![](http://docs.mlsql.tech/upload_images/WX20190819-183545.png）

如果你在添加实例的时候，忘记了添加某些角色，你还可以在[Add role to backend]面板中追加。


## 查看已经添加的实例

![](http://docs.mlsql.tech/upload_images/WX20190819-183658.png)

## 将自己和特定实例绑定


![](http://docs.mlsql.tech/upload_images/WX20190819-183826.png)

## 添加表权限

该功能位于菜单导航栏Team下的左侧边栏的Team标签下：

![](http://docs.mlsql.tech/upload_images/WX20190819-184043.png)

举个例子，我现在要访问如下hive表hive_table_1:

```sql
load hive.`hive_table_1` as table1;
select * from table1 as output;
```
点击运行，系统会告诉你没有权限访问。因为在MLSQL Console中，除了主目录的文件以及临时表，其他的都是需要显示设置权限才能访问的。

```sql
backend status:500

Error:

db:    default
table: hive_table_1
tableType: hive
sourceType: hive
operateType: load

is not allowed to access.
           
java.lang.RuntimeException: 
Error:
```

为了能够访问该表，首先，你要将该表添加到Team里：

![](https://upload-images.jianshu.io/upload_images/WX20190819-184416.png)

接着添加到对应的Role里，这个时候你还需要选择需要对这个表进行什么操作。我这里选择了load权限。

![](https://upload-images.jianshu.io/upload_images/WX20190819-184458.png)

现在，就可以访问该表了。


