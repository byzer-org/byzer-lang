# Select 语法

Select语法是我们处理数据最重要的方式之一。之所以说之一，是因为我们还有ET(应用于run/train/predict)。不过Select语法是一个通用的数据处理方案，
足够灵活并且功能强大。

## 基本语法

最简单的一个select语句：

```sql
select 1 as col1 
as table1;
```

从上面可以看到，MLSQL中的select语法和传统SQL中的select语法唯一的差别就是后面多了一个 `as tableName`。这也是为了方便后续继续对该SQL处理的结果进行
处理而引入的微小改良。 

在后续讲解register语法的章节，我们会看到，如何通过scala/java/python来增加SQL中的可用的自定义函数。