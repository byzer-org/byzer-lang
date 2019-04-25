# How to send mail 

After the etl job finishing, we should push a notice to people.
You can use SendMessage to achieve this target:


```sql

set EMAIL_TITLE = "subject";
set EMAIL_BODY = `select download_url from t1` options type = "sql";
set EMAIL_TO = "";

select "${EMAIL_BODY}" as content as data;

run data as SendMessage.``
where method="mail"
and to = "${EMAIL_TO}"
and subject = "${EMAIL_TITLE}"
and smtpHost = "xxxxxxxx";

```

Another example:

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

download_csv_url is a udf which can generate a download url.
    