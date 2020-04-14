## 常见函数

### sleep 

休眠函数，方便调试。

```sql
-- 休眠1s
select sleep(1000)
```

### uuid 

产生一个唯一的字符串，去掉了"-"

```sql
-- 休眠1s
select uuid()
```

使用场景如生成一张随机表：

```
set table = `select uuid()` options type="sql";
```

### matrix_array
 
矩阵转数组

```sql
select matrix_array(array_onehot(array(1,2),12))
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

### vec_slice 

切割vector:


```sql
select vec_slice(vec_dense(array(1.0,2.0,3.0)),array(0,1))
```

### vec_array

把向量转化为数组

```sql
select vec_array(vec_dense(array(1.0,2.0))) 
```

### vec_mk_string

把向量进行拼接

```sql
select vec_mk_string(vec_dense(array(1.0,2.0))) 
```

### vec_wise_mul

```sql
select vec_dense(cast(array(2.5, 2.0, 1.0) as array<double>)) as f as data;
select vec_wise_mul(f, f) as nf from data;
```

```json
[
    {
        "nf": {
            "type": 1,
            "values": [
                6.25,
                4.0,
                1.0
            ]
        }
    }
]
```

### vec_wise_add

```sql
select vec_dense(cast(array(2.5, 2.0, 1.0) as array<double>)) as f as data;
select vec_wise_add(f, f) as nf from data;
```

```json
[
    {
        "nf": {
            "type": 1,
            "values": [
                5.0,
                4.0,
                2.0
            ]
        }
    }
]
```

### vec_wise_dif

```sql
select vec_dense(cast(array(2.5, 2.0, 1.0) as array<double>)) as f1, vec_dense(cast(array(2.5, 22.2, 1.6) as array<double>)) as f2 as data;
select vec_wise_dif(f1, f2) as nf from data;
```

```json
[
    {
        "nf": {
            "type": 1,
            "values": [
                0.0,
                -20.2,
                -0.6000000000000001
            ]
        }
    }
]
```

### vec_wise_mod

```sql
select vec_dense(cast(array(2.5, 2.0, 1.0) as array<double>)) as f1, vec_dense(cast(array(2.5, 4.0, 3.0) as array<double>)) as f2 as data;
select vec_wise_mod(f1, f2) as nf from data;
```

```json
[
    {
        "nf": {
            "type": 1,
            "values": [
                0.0,
                2.0,
                1.0
            ]
        }
    }
]
```

### vec_inplace_add

```sql
select vec_dense(cast(array(2.5, 2.0, 1.0) as array<double>)) as f as data;
select vec_inplace_add(f, 4.4) as nf from data;
```

```json
[
    {
        "nf": {
            "type": 1,
            "values": [
                6.9,
                6.4,
                5.4
            ]
        }
    }
]
```

### vec_inplace_ew_mul

```sql
select vec_dense(cast(array(2.5, 2.0, 1.0) as array<double>)) as f as data;
select vec_inplace_ew_mul(f, 4.4) as nf from data;
```

```json
[
    {
        "nf": {
            "type": 1,
            "values": [
                11.0,
                8.8,
                4.4
            ]
        }
    }
]
```

### vec_ceil

```sql
select vec_dense(cast(array(2.5, 2.4, 1.6) as array<double>)) as f as data;
select vec_ceil(f) as nf from data;

```
```json
[
    {
        "nf": {
            "type": 1,
            "values": [
                3.0,
                3.0,
                2.0
            ]
        }
    }
]
```

### vec_floor

```sql
select vec_dense(cast(array(2.5, 2.4, 1.6) as array<double>)) as f as data;
select vec_floor(f) as nf from data;

```
```json
[
    {
        "nf": {
            "type": 1,
            "values": [
                2.0,
                2.0,
                1.0
            ]
        }
    }
]
```

### vec_mean

向量平均值

```sql
select vec_mean(vec_dense(array(1.0,2.0,7.0, 2.0)))
```

```json
[
    {
        "UDF:vec_mean(UDF:vec_dense(cast(array(1.0, 2.0, 7.0, 2.0) as array<double>)))": 3.0
    }
]
```

### vec_stddev

向量标准差

```sql
select vec_stddev(vec_dense(array(3.0, 4.0, 5.0)))
```

```json
[
    {
        "UDF:vec_stddev(UDF:vec_dense(cast(array(3.0, 4.0, 5.0) as array<double>)))": 1.0
    }
]
```

### ngram

```sql
select ngram(array("a","b","c","d","e"),3) as 3ngr
```

### array_intersect

```sql
select array_intersect(array("a","b","c","d","e"),array("a")) as k
```

### array_index

```sql
select array_index(array("a","b","c","d","e"),"b") as k
```

### array_slice

```sql
select array_slice(array("a","b","c","d","e"),3,-1) as k
```

### array_number_concat

多个数组拼接成一个数组，并且展开。比如 [[1,2],[2,3]] => [1,2,2,3]

### array_concat

同array_number_concat，支持元素类型为字符串

### array_number_to_string

对数组内的元素做类型转换

### array_string_to_double

对数组内的元素做类型转换

### array_string_to_float

对数组内的元素做类型转换

### array_string_to_int

对数组内的元素做类型转换

---

### matrix_dense

生成一个紧凑矩阵

```sql
select matrix_dense(array(array(1.0, 2.0, 3.0), array(2.0, 3.0, 4.0)))
```

```json
[
    {
        "UDF:matrix_dense(cast(array(array(1.0, 2.0, 3.0), array(2.0, 3.0, 4.0)) as array<array<double>>))": {
            "isTransposed": false,
            "numCols": 3,
            "numRows": 2,
            "type": 1,
            "values": [
                1.0,
                2.0,
                2.0,
                3.0,
                3.0,
                4.0
            ]
        }
    }
]
```

### matrix_sum

```sql
select matrix_sum(matrix_dense(array(array(1.0, 2.0, 3.0), array(2.0, 3.0, 4.0))), 0)
```

```json
[
    {
        "UDF:matrix_sum(UDF:matrix_dense(cast(array(array(1.0, 2.0, 3.0), array(2.0, 3.0, 4.0)) as array<array<double>>)), 0)": {
            "type": 1,
            "values": [
                3.0,
                5.0,
                7.0
            ]
        }
    }
]
```

---

### keepChinese

对文本字段做处理，只保留中文字符

```
set query = "你◣◢︼【】┅┇☽☾✚〓▂▃▄▅▆▇█▉▊▋▌▍▎▏↔↕☽☾の·▸◂▴▾┈┊好◣◢︼【】┅┇☽☾✚〓▂▃▄▅▆▇█▉▊▋▌▍▎▏↔↕☽☾の·▸◂▴▾┈┊啊，..。，！?katty"
select keepChinese("${query}",false,array()) as jack 
as chitable
-- 结果: 你好啊   
```

第二个参数如果为true,则会保留一些常见的标点符号，第三个参数可以指定特定哪些字符需要保留字符。

### 度量函数

todo
