# /run/script 接口

该接口用来执行MLSQL语句。

## 参数列表

| 参数 | 说明 | 示例值 |
|----|----|-----|
| sql  | 需要执行的MLSQL内容  |     |
| owner  | 当前发起请求的租户  |     |
| jobType  | 任务类型 script/stream/sql  默认script |     |
| executeMode  | 如果是执行MLSQL则为query,如果是为了解析MLSQL则为analyze。很多插件会提供对应的executeMode从而使得用户可以通过HTTP接口访问插件功能 |     |
| jobName  | 任务名称，一般用uuid或者脚本id,最好能带上一些信息，方便更好的查看任务 |     |
| timeout  | 任务执行的超时时间| 单位毫秒    |
| silence  | 最后一条SQL是否执行 | 默认为 false|
| sessionPerUser  | 按用户创建sesison| 默认为 true|
| sessionPerRequest  | 按请求创建sesison| 默认为 false,一般如果是调度请求，务必要将这个值设置为true|
| async  | 请求是不是异步执行| 默认为 false|
| callback  | 如果是异步执行，需要设置回调URL| |
| skipInclude  | 禁止使用include语法| 默认false |
| skipAuth  | 禁止权限验证| 默认true  |
| skipGrammarValidate  | 跳过语法验证| 默认true  |
| includeSchema  | 返回的结果是否包含单独的schema信息| 默认false  |
| fetchType  | take/collect, take在查看表数据的时候非常快| 默认collect  |
| defaultPathPrefix  | 所有用户主目录的基础目录|   |
| `context.__default__include_fetch_url__`  | Engine获取include脚本的地址| |
| `context.__default__console_url__` | console地址|   |
| `context.__default__fileserver_url__` | 下载文件服务器地址，一般默认也是console地址|   |
| `context.__default__fileserver_upload_url__` | 上传文件服务器地址，一般默认也是console地址|   |
| `context.__auth_client__` | 数据访问客户端的class类|  默认是streaming.dsl.auth.meta.client.MLSQLConsoleClient |
| `context.__auth_server_url__` | 数据访问验证服务器地址|   |
| `context.__auth_secret__` | engine回访请求服务器的密钥。比如console调用了engine，需要传递这个参数， 然后engine要回调console,那么需要将这个参数带回|   |







