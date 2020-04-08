# Excel数据源插件

【文档更新日志：2020-04-07】

> Note: 本文档适用于MLSQL Engine 1.6.0-SNAPSHOT/1.6.0 及以上版本。  
> 对应的Spark版本应该为2.4.5,不低于2.4.3。

## 作用

该插件用于加载Excel数据源。

## 安装

```
!plugin ds add - "mlsql-excel-2.4";
```

## 使用示例

```sql
load excel.`/tmp/upload/example_en.xlsx` 
where useHeader="true" 
and maxRowsInMemory="100" 
and dataAddress="A1:C8"
as data;

select * from data as output;
```

参数介绍：

1. useHeader 是否有表头
2. maxRowsInMemory 控制内存
3. dataAddress Excel表里的起始和结束位置。分别为最左上角和最右下角。

## 项目地址

[mlsql-excel](https://github.com/allwefantasy/mlsql-plugins/tree/master/mlsql-excel)