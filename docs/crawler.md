StreamingPro crawler 提供了crawlersql 数据源,可以抓取入口页的url,
同时也提供了一系列函数 ，方便对网页进行爬取，解析。

## crawlersql 数据源

crawlersql 应用在load语法中。

```
load crawlersql.`https://www.csdn.net/nav/ai` 
options matchXPath="//ul[@id='feedlist_id']//div[@class='title']//a/@href" 
and fetchType="list"
and tempStore="/tmp/streamingpro_crawler/c61c326a525ce1ddb672147e0096ef26"
and phantomJSPath="/usr/local/Cellar/phantomjs/2.1.1/bin/phantomjs"
and `page.type`="scroll"
and `page.num`="10"
and `page.flag`="feedlist_id"
as aritle_url_table_source;
```
 
aritle_url_table_source 是加载完成后的别名，该表有两个字段，url和root_url。 root_url也就是https://www.csdn.net/nav/ai 。

options 参数主要有：


| 参数名	 | 描述  |备注 |
|:-----------|:------------|:------------|
|matchXPath | xpath语法，提取url列表 ||
|fetchType | 获取一个列表或者单篇内容。list/single||
|tempStore |  url暂存目录，方便做去重，比如已经爬取过的url就不再爬取||
|phantomJSPath | 可选，如果page.type 为scroll,则需要使用phantomJSP去抓，因为要进行ajax下拉||
|page.type | 可选，paging/scroll,默认为paging,表示传统分页||
|page.num | 可选，翻多少页||
|page.flag | 抓取时页面出现什么标记的时候，才算成功||

## 爬虫相关UDF函数

### crawler_request

页面抓取函数。

```sql
select crawler_request(url)
```

### crawler_browser_request

页面抓取函数，但是是使用phantomJS进行抓取，可以执行js脚本。c_flag表示某个id元素。如果不像设置，可以设置为""即可。

```sql
select crawler_browser_request(url,phantomJSPath,c_flag)
```

### crawler_auto_extract_title

给定html,抽取标题

```sql
select crawler_auto_extract_title(html)
```

### crawler_auto_extract_body

给定html,抽取正文

```sql
select crawler_auto_extract_body(html)
```



### crawler_extract_xpath

给定html,抽取指定xpath下的文本

```sql
select crawler_extract_xpath(html,"//main/article//span[@class='time']") as created_time
```

### crawler_md5

对指定字符串做md5

```sql
select crawler_md5(url) as md5_url
```


## 一个完整示例

本脚本会抓取csdn ai 板块下的博文，并且自动下拉20次，大概得到上百条内容。最后解析出时间，标题，内容，然后保存
到MySQL里。


```sql
set resultTempStore="/tmp/streamingpro_crawler_content";
set tempStore="/tmp/streamingpro_crawler/c61c326a525ce1ddb672147e0096ef26";

--注册一张mysql数据库表
connect mysql where url="..."
and driver=""
and user=""
and password=""
as mysql_crawler_db;


-- 抓取列表页的url
load crawlersql.`https://www.csdn.net/nav/ai` 
options matchXPath="//ul[@id='feedlist_id']//div[@class='title']//a/@href" 
and fetchType="list"
and tempStore="/tmp/streamingpro_crawler/c61c326a525ce1ddb672147e0096ef26"
and phantomJSPath="/usr/local/Cellar/phantomjs/2.1.1/bin/phantomjs"
and `page.type`="scroll"
and `page.num`="10"
and `page.flag`="feedlist_id"
as aritle_url_table_source;

-- 抓取全文，并且存储
select crawler_request(regexp_replace(url,"http://","https://"))  as html 
from aritle_url_table_source 
where url is not null
as aritle_list;
save overwrite aritle_list as parquet.`${resultTempStore}`;

-- 对内容进行解析
load parquet.`${resultTempStore}` as aritle_list;
select 
crawler_auto_extract_title(html) as title,
crawler_auto_extract_body(html) as body,
crawler_extract_xpath(html,"//main/article//span[@class='time']") as created_time
from aritle_list 
where html is not null
as article_table;

-- 对最后的抓取结果保存到mysql里
save append article_table as jdbc.`mysql_crawler_db.crawler_table`;

-- 已经抓取过的url也需要进行增量存储，方便后续过滤
save append aritle_url_table_source as parquet.`${tempStore}`;

```

运行时，需要先保证tempStore不能为空，你可以通过下面脚本初始化：

```
select "" as url ,"" as root_url 
as em;
save overwrite em parquet.`/tmp/streamingpro_crawler/c61c326a525ce1ddb672147e0096ef26`
```