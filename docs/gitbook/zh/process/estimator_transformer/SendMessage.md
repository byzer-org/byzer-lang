# 如何发送邮件

数据处理完成后，我们可以邮件通知业务方。  

使用场景示例：
  1、数据计算处理后，生成下载链接，发邮件给相关人员
  2、数据量较少的情况，可以直接发送数据处理结果
  
发送邮件使用run语法，方式如下：

```sql

set EMAIL_TITLE = "这是邮件标题";
set EMAIL_BODY = `select download_url from t1` options type = "sql";
set EMAIL_TO = "";

select "${EMAIL_BODY}" as content as data;

run data as SendMessage.``
where method="mail"
and to = "${EMAIL_TO}"
and subject = "${EMAIL_TITLE}"
and smtpHost = "xxxxxxxx";

```

下面给一个完整的例子：

```sql

select download_csv_url("${DATA_FILE_PATH}") as download_url
as t1;


set EMAIL_TITLE = "这是邮件标题";
set EMAIL_BODY = `select download_url from t1` options type = "sql";
set EMAIL_TO = "";

select "${EMAIL_BODY}" as content as data;

run data as SendMessage.``
where method="mail"
and to = "${EMAIL_TO}"
and subject = "${EMAIL_TITLE}"
and smtpHost = "xxxxxxxx";

```

其中 download_csv_url 是一个udf函数，他会使用一个接口服务根据路径获得一个下载地址，之后将这个地址发送给需要接收的人。
    