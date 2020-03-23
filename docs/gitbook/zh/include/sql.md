# 如何简化一条SQL语句



我们来看下如下一条SQL语句：

```sql
select  created_at,
        case a.channel_name
             when "推荐" then "v1"
             when "热门" then "v2"
             when "关注" then "v3"
             else "unknow"
        end  as m,
        count(distinct mc) as ma
from table1 a
where a.date='${YESTERDAY}'
and  ...
group by
dt, case a.channel_name
                 when "推荐" then "v1"
                 when "热门" then "v2"
                 when "关注" then "v3"
                 else "unknow"
            end  
as final_table;          
```

我们看到这里有两个case when,但是是一模一样的。如果我修改了一个地方，就需要同时去修改另外一个地方。而且，
这个case when 其实不止这一条SQL会使用。在这之前，我们是没有办法解决这个问题的。在MLSQL中，我们提供了一套机制
来简化这种场景。假设我们这条sql语句所在的脚本叫 `a.mlsql`

我们添加一个新的文件，叫`b.mlsql`,内容如下：


```sql
set tableName="m" where type="default";
set channel_name='''
case ${tableName}.channel_name
     when "推荐" then "v1"
     when "热门" then "v2"
     when "关注" then "v3"
     else "unknow"
end     
''';
```

这里我们看到了set语法一个特殊配置， `type="default"`。 default值专门用于配合脚本include的。如果引用脚本
包含了tableName这个变量，那么该参数不会生效，如果引用脚本没有包含tableName,那么该参数会生效，在channel_name里
对应的值会是`m.channel_name`。

现在，我们修改下`a.mlsql`:

```sql
set tableName="a";
include project.`b.mlsql`;

select  created_at,
        ${channel_name}  as m,
        count(distinct mc) as ma
from table1 a
where a.date='${YESTERDAY}'
and  ...
group by
dt, ${channel_name}  
as final_table;          
```

现在我们看，脚本得到了很大的简化，并且通过设置tableName，可以将b.mlsql嵌入到任何需要该脚本的其他脚本里，复用性得到很大
的增强。

通过这种方式，我们可以将一条复杂的SQL进行拆解，从而模块化SQL语句。

当然，对于复杂的join语句，我们建议大家多少使用中间表，用多条SQL语句而不是一条SQL完成任务。
