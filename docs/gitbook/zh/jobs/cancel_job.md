# 取消正在运行任务

前面比较核心的是JobName 和groupId,我们可以使用这两个字段其中一个找到具体的任务并且取消掉他。

具体取消的方式为：

```sql
select 1 as a as EMPTY_TABLE;
run EMPTY_TABLE as MLSQLJobExt.`` where groupId="23"; 
```