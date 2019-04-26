## Operate MySQL directly

As mentioned in preview chapter about JDBC datasource, we can operate DDL/Query on MySQL.
Here are examples: 

```sql
set user="root";
 
 connect jdbc where
 url="jdbc:mysql://127.0.0.1:3306/wow?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false"
 and driver="com.mysql.jdbc.Driver"
 and user="${user}"
 and password="${password}"
 as db_1;


select 1 as a as FAKE_TABLE;

run command as JDBC.`db_1` where 
`driver-statement-0`="drop table test1"
and `driver-statement-1`="create table test1.....";
```
driver-statement-[number], the number in value is the order of executing.


