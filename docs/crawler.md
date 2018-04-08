streamingpro crawler 提供了crawlersql 数据源。同时也提供了一系列函数 。

## crawler_request



```sql
set resultTempStore="/tmp/streamingpro_crawler_content";
set tempStore="/tmp/streamingpro_crawler/c61c326a525ce1ddb672147e0096ef26";

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

-- 对最后的抓取结果进行保存
save overwrite article_table as json.`/tmp/article_table`;

-- 已经抓取过的url也需要进行增量存储，方便后续过滤
save append aritle_url_table_source as parquet.`${tempStore}`;

```