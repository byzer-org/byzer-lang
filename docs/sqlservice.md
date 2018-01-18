## 启动一个SQLServer服务

StreamingPro极大的简化了SQL Server，除了thrift server以外，它也支持使用Rest形式的接口。你唯一需要做的就是准备一个只包含

```
{}
```
的query.json的文件（名字可以任意），然后按如下的方式启动即可：

```
SHome=/Users/allwefantasy/streamingpro

./bin/spark-submit   --class streaming.core.StreamingApp \
--master local[2] \
--name sql-interactive \
$SHome/streamingpro-spark-2.0-0.4.15-SNAPSHOT.jar    \
-streaming.name sql-interactive    \
-streaming.job.file.path file://$SHome/query.json \
-streaming.platform spark   \
-streaming.rest true   \
-streaming.driver.port 9003   \
-streaming.spark.service true \
-streaming.thrift true \
-streaming.enableHiveSupport true
```

启动后，我们可以测试下一下，我们先通过接口创建一张表：

```
//CREATE TABLE IF NOT EXISTS zhl_table(id string, name string, city string, age Int)
curl --request POST \
  --url http://127.0.0.1:9003/run/sql \
  --data 'sql=CREATE%20TABLE%20IF%20NOT%20EXISTS%20zhl_table(id%20string%2C%20name%20string%2C%20city%20string%2C%20age%20Int)%20'
```

然后创建一个csv格式的数据，然后按如下方式导入：

```
//LOAD DATA LOCAL INPATH  '/Users/allwefantasy/streamingpro/sample.csv'  INTO TABLE zhl_table
curl --request POST \
  --url http://127.0.0.1:9003/run/sql \
  --data 'sql=LOAD%20DATA%20LOCAL%20INPATH%20%20'\''%2FUsers%2Fallwefantasy%2Fstreamingpro%2Fsample.csv'\''%20%20INTO%20TABLE%20zhl_table'
```

然后你就可以查询了：

```
//sql: SELECT * FROM zhl_table
curl --request POST \
  --url http://127.0.0.1:9003/run/sql \
  --data 'sql=SELECT%20*%20FROM%zhl_table'
```

### JDBC查询

当然，因为我们开启了thrift server，你也可以写程序链接这个服务：

```
object ScalaJdbcConnectSelect {

  def main(args: Array[String]) {
    // connect to the database named "mysql" on the localhost
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:hive2://localhost:10000/default"

    // there's probably a better way to do this
    var connection:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      val resultSet = statement.executeQuery("SELECT * FROM zhl_table ")
      while ( resultSet.next() ) {
        println(" city = "+ resultSet.getString("city") )
      }
    } catch {
      case e => e.printStackTrace
    }
    connection.close()
  }

}
```

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
