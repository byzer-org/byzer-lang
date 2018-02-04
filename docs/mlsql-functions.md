## 机器学习相关UDF函数

### sleep 

休眠函数，方便调试。

```sql
-- 休眠1s
select sleep(1000)
```

### vec_argmax

找到向量里面最大值所在的位置。

```sql
-- 休眠1s
select vec_argmax(vec_dense(array(1.0,2.0,7.0))) as label
```

### vec_dense

生成一个紧凑向量

```sql
select vec_dense(array(1.0,2.0,7.0))
```

### vec_sparse

生成一个稀疏向量

```sql
select vec_sparse(2,--此处传递一个map)

```

### vec_concat

拼接多个向量成为一个向量
 
 ```sql
 select vec_concat(
     array(
     vec_dense(array(1.0)),
     vec_dense(array(1.0))
     )
 )
 ```
 
### vec_cosine
 
计算consine 向量夹角

```sql
select vec_cosine(vec_dense(array(1.0,2.0)),vec_dense(array(1.0,1.0)))
```

### ngram

```
select ngram(array("a","b","c","d","e"),3) as 3ngr
```