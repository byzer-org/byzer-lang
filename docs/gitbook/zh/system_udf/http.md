# http 请求


http请求可以让MLSQL脚本变得更加强大，因为这可以集合所有内部或者外部API来完成某项工作。
MLSQL提供了一个支持较为全面http请求函数。

比如：

```sql
select crawler_http("http://www.csdn.net","GET",map("k1","v1","k2","v2")) as c as output;
```

其中GET部分支持： 

* GET
* POST

MLSQL也提供了图片请求功能：

```sql
select crawler_request_image("http://www.csdn.net","GET",map("k1","v1","k2","v2")) as c as output;
```

这个时候c字段是一个array[byte]类型。

我们提供了简单的标题和内容抽取函数：

* crawler_auto_extract_body
* crawler_auto_extract_title

给定一段文本，即可完成。

也支持xpath,对应的函数为 crawler_extract_xpath(html,xpath)。


