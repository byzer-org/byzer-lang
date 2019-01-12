# QQ/电话/邮件抽取

FeatureExtractInPlace 是个比较有趣的ET,他可以帮你对文本里的类似QQ,电话抽取出来，在反垃圾方面比较有用。

```sql

set rawData='''
{"content":"请联系 13634282910","predict":"rabbit"}
{"content":"扣扣 527153688@qq.com","predict":"dog"}
{"content":"<html> dddd img.dxycdn.com ffff 527153688@qq.com","predict":"cat"} 
''';
load jsonStr.`rawData` as data;
```

接下来我们算算看：

```sql
train data as FeatureExtractInPlace.`/tmp/model`
where inputCol="content";

load parquet.`/tmp/model/data` as output;
```

下边是结果：

```
content predict phone  noEmoAndSpec  email  qqwechat url pic cleanedDoc blank chinese english number punctuation uninvisible  mostchar  length
请联系 13634282910	rabbit		请联系 13634282910			0	0	请联系 13634282910	6	20	0	73	0	0	2	15
扣扣 527153688@qq.com	dog		扣扣 527153688@qq.com			0	0	扣扣	33	66	0	0	0	0	2	3
<html> dddd img.dxycdn.com ffff 527153688@qq.com	cat		<html> dddd img.dxycdn.com ffff 527153688@qq.com			0	1	dddd ffff	27	0	72	0	0
```

内容很丰富。

## 如何在预测时使用

任何ET都具备在"训练时学习到经验"转化为一个函数，从而可以使得你把这个功能部署到流式计算，API服务里去。同时，部分ET还有batch predict功能，
可以让在批处理做更高效的预测。

对于ET FeatureExtractInPlace 而言，我们要把它转化为一个函数非常容易：

```sql

register FeatureExtractInPlace.`/tmp/model` as convert;

```

该ET比较特殊，会隐式的给你生成非常多的函数：

1. convert_phone  
1. convert_email 
1. convert_qqwechat 
1. convert_url the number of url
1. convert_pic  the number of pic
1. convert_blank  blank percentage
1. convert_chinese chinese percentage,
1. convert_english english percentage,
1. convert_number  number percentage,
1. convert_punctuation  punctuation percentage,
1. convert_mostchar mostchar percentage,
1. convert_length text length

你可以根据需求使用。比如：

```sql

select convert_qqwechat("扣扣 527153688@qq.com ") as features as output;
```

输出结果为：

```
features
true
```

