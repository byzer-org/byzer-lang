#Select 语法

MLSLQ中 select语法其实就是标准的select 语句然后加上 一个 as [tableName]; 
因为在MLSQL看来，select语句本质是transformer,把数据从原表处理成新表。

看起来会是这样：

```sql
set rawData=''' 
{"jack":1,"jack2":2}
{"jack":2,"jack2":3}
''';

load jsonStr.`rawData` as table1;

select jack+2 as newjack from table1 -- 标准的sql 
as table2; -- sql处理后，我们给这条sql产生的数据取个名字

-- 我可以引用前面申明了的table2继续做处理
select * from table2 as table3; 

```

通过组合大量这种语句，我们可以避免去写非常复杂的SQL语句。每条SQL都会产生一张新的表，这些表可以在后续的SQL里继续使用，从而
降低SQL的复杂度。
