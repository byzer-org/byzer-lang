#混淆矩阵

混淆矩阵在分类算法里用处很多，可以让你直观看到数据的错误分布情况。他主要是将每个分类的实际值和预测值形成一个矩阵，这样
我们就可以很清楚的知道错误情况了。

假设我们有动物分类，有两列，一列是实际值，一列是预测值，内容如下：

```sql

set rawData='''
{"label":"cat","predict":"rabbit"}
{"label":"cat","predict":"dog"}
{"label":"cat","predict":"cat"}
{"label":"dog","predict":"dog"}
{"label":"cat","predict":"dog"} 
''';
load jsonStr.`rawData` as data;
```

接着我们使用混淆矩阵来查看最后的预测结果分布：

```sql
train data as ConfusionMatrix.`/tmp/model` 
where actualCol="label" 
and predictCol="predict";

load parquet.`/tmp/model/data` as output;
```

结果如下：

```
act\prt  cat dog rabbit
cat    	 1	 2	 1
dog	     0	 1	 0
rabbit	 0	 0	 0
```

另外我们也可以看到一些统计值：

```sql
load parquet.`/tmp/model/detail` as output;
```

结果如下：

```
lable name  value       desc
cat	  TP	1	        True positive [eqv with hit]
cat	  TN	1	        True negative [eqv with correct rejection]
cat	  FP	0	        False positive [eqv with false alarm, Type I error]
cat	  FN	3	        False negative [eqv with miss, Type II error]
......

```








