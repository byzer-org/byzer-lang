# 如何管理connect账号和密码

【文档更新日志：2020-04-10】

> Note: 本文档适用于MLSQL Engine 1.3.0 及以上版本。  
> 对应的Spark版本可支持2.3.2/2.4.3

通常，我们会把所有账号密码用connect语法申明在某个文件里。正常情况，这些文件会属于特定某个账号，
用户需要登录才能看得到。一般而言不会造成问题。

但是，如果用户需要把脚本完整备份出来，比如备份到版本库，那么在版本库中便存在明文密码了，一旦版本库
泄露就有可能造成很大的危害。

所以我们给的建议是，将账号密码保存在

1. HDFS文件系统中
2. 或者账号密码API服务里

对于文件系统中，可以这样使用：

```sql
load json.`user/password信息路劲` as auth_info_table;
set user="select user from auth_info_table" where type="sql";
set password="select password from auth_info_table" where type="sql";

connect jdbc.`` where user="${user}"
and password=${password};
```

这样既可避免敏文密码存在在脚本中。

第二种方式是：

```sql

select crawler_http("http://auth....","GET",map("owner","v1")) as authInfo as auth_info_table;

set user="select get_json(authInfo,'user') from auth_info_table" where type="sql";
set password="select get_json(authInfo,'password') from auth_info_table" where type="sql";

connect jdbc.`` where user="${user}"
and password=${password};
```

这种connect脚本必须是属于管理员的，同时将其设置为启动时执行，这样其他租户可以无感访问临时表（其实数据来源于比如数据库）。
