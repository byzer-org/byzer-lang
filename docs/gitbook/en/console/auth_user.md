# Privileges and User Management

After registering MLSQL Console, any user can set up their own Team, Role and Cluster. There is no so-called Supre authority
Although MLSQL Engine implements column-level Data permission control, MLSQL Console does not support column-level settings until version 1.4.0 at this stage.
The main nouns are explained below:

1. Team     
2. Role     
3. User     
4. Backend  Mainly refers to MLSQL Engine instances
5. Table    Table, entity of authority

In MLSQL, there are several constraints as follows:



1. Backend (MLSQL Engine instance) is bound to one or more Roles. Each binding automatically generates a label in the format 'team_role'.
2. User is also bound to one or more Roles, each binding automatically generates a label in the form of 'team_role'.
3. User with the same label can access the corresponding Cluster
4. At the same time, users can only access the backend of one tag, default backend tag.

In other words, the following settings are required to access the MLSQL Console system.

1. Create Team/Role
2. Add yourself to the created Team/Role
3. Add Backend (MLSQL Engine) instance
4. Add Backend to Team/Role
5. Set the default backend tag for a team_role
6. Users can execute ML SQL statements on the home page

Team/Setup provides a quick wizard for personal use. See [Quick Configuration Wizard](http://docs.mlsql.tech/zh/installation/docker.html) for details. If you don't use Docker, you need to fill in the corresponding domain name and port when creating Backend.


## Create Team

![](http://docs.mlsql.tech/upload_images/WX20190819-182306.png)


After the creation is completed, you can see the newly created Team in Team belongs to you.

![](http://docs.mlsql.tech/upload_images/WX20190819-182602.png)

## Invite members to join your team
  

![](http://docs.mlsql.tech/upload_images/WX20190819-182747.png)

如果你发现下拉款没有你新建的组或者角色或者其他的时候，尝试点击Setup然后再点击回来（触发刷新）。

比如当前jack2里只有我自己：

![](http://docs.mlsql.tech/upload_images/WX20190819-182959.png)

现在我点击Create按钮后，jack2里就有新用户了。如果该用户不存在，上面会弹出提示说用户不存在。请仔细检查。

![](http://docs.mlsql.tech/upload_images/WX20190819-183109.png)

邀请后，被邀请用户需要确认，该用户可以到 [Team inviting you] 面板中点击拒绝或者同意。

## Manage roles for Team
   
![](http://docs.mlsql.tech/upload_images/WX20190819-183340.png)

## Add users to roles  

  
![](http://docs.mlsql.tech/upload_images/WX20190819-183340.png)

## Add MLSQL Engine instance

![](http://docs.mlsql.tech/upload_images/WX20190819-183545.png)


## View Existing Instances

![](http://docs.mlsql.tech/upload_images/WX20190819-183658.png)

## Bind users and instances


![](http://docs.mlsql.tech/upload_images/WX20190819-183826.png)

## Add table permissions

![](http://docs.mlsql.tech/upload_images/WX20190819-184043.png)

For example, access hive table hive_table_1:

```sql
load hive.`hive_table_1` as table1;
select * from table1 as output;
```
Clicking runs will throw exceptions that do not have permission to access.

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

