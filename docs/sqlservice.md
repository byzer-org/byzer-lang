### 异步查询

有的时候，spark计算时间非常长，我们希望任务丢给spark计算，然后计算好了，再通知我们，streamingpro也支持这种功能。具体做法
如下：

```
curl --request POST \
  --url http://127.0.0.1:9003/run/sql \
  --data 'sql=select%20*%20from%20zhl_table&async=true&resultType=file&path=%2Ftmp%2Fjack&callback=http%3A%2F%2F127.0.0.1%3A9003%2Fpull'
```

[](http://upload-images.jianshu.io/upload_images/1063603-230d2c4387b8903c.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

具体参数如下：

| 参数名称	 | 默认值  |说明 |
|:-----------|:------------|:------------|
|async| false|是否异步执行||
|sql | 查询SQL||
|path| 无|spark的查询结果会先临时写入到这个目录|
|callback| 无|StreamingPro会调用该参数提供的接口告知下载地址。|
|tableName| 无|如果该参数被配置，那么数据会被写入对应的hive表|
|resultType| 无|async=false时，如果该参数被设置并且file时，那么接口返回一个地址而非结果|

### XQL查询语法

StreamingPro Rest服务除了支持SQL语句外也有自己的类SQL语法，一个典型的脚本如下：

```
//链接一个mysql 数据库,并且将该库注册为db1
connect jdbc 
     where 
    driver="com.mysql.jdbc.Driver"
    and url="jdbc:mysql://127.0.0.1/db?characterEncoding=utf8"
    and user="root"
    and password="****"
    as db1;
//加载t_report表为tr
load jdbc.`db1.t_report` as tr;
// 把tr表处理完成后映射成new_tr表
select * from tr  as new_tr;
//保存到/tmp/todd目录下，并且格式为json
save new_tr as json.`/tmp/todd`;
```

你需要访问`/run/script`接口，并且通过参数sql提交上面的脚本内容：

```
curl --request POST \
  --url http://127.0.0.1:9003/run/script \
  --data 'sql=上面的脚本内容'
```

我建议用PostMan之类的工具做测试。之后用`/run/sql`来查看结果

```
curl --request POST \
  --url http://127.0.0.1:9003/run/sql \
  --data 'sql=select * from  json.`/tmp/todd`'
```
