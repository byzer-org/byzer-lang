# HTTP UDFs

Before you can use this functions, please add this line to your startup script:

```sql
 -streaming.udf.clzznames streaming.crawler.udf.Functions
```

HTTP UDFs make MLSQL more powerful, this means you can invoke any API from out or inner to help you achieve your target.

For example:

```sql
select crawler_http("http://www.csdn.net","GET",map("k1","v1","k2","v2")) as c as output;
```

The second parameter supports:

* GET
* POST

MLSQL also support download image:

```sql
select crawler_request_image("http://www.csdn.net","GET",map("k1","v1","k2","v2")) as c as output;
```

c is array[byte].

We also provide UDFs which you can used to extract title and body from html:

*  crawler_auto_extract_body
*  crawler_auto_extract_title

Or you can use xpath to extract something you want:

```sql

crawler_extract_xpath(html,xpath)
```
