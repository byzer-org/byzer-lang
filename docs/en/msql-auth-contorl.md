## MLSQL Auth Control Mechanism

MLSQL Auth Control Mechanism is developing actively now. 
StreamingPro does not save any auth information by itself. 
But it will provide tables you accessed in MLSQL script,
and invoke implementation you provide  to give you a opportunity to check weather these tables are
allowed to access.

We will show you how to do that now.

Step1: Introduce the streamingpro-api to your project.

Step2: Create a class named `streaming.dsl.auth.meta.client.DefaultConsoleClient`, this class contains:

```scala
package streaming.dsl.auth.meta.client

import streaming.dsl.auth.{MLSQLTable, TableAuth, TableAuthResult}
import streaming.log.{Logging, WowLog}

/**
  * Created by allwefantasy on 11/9/2018.
  */
class DefaultConsoleClient extends TableAuth with Logging with WowLog {
  override def auth(tables: List[MLSQLTable]): List[TableAuthResult] = {
    logInfo(format(s"auth ${tables.mkString(",")}"))
    throw new RuntimeException("auth fail")
  }
}
```

Suppose you run MLSQL script like this:

```sql
set __auth_client__="streaming.dsl.auth.meta.client.DefaultConsoleClient";
load parquet.`/tmp/abc` as newtable;
select * from default.abc as cool;
```

In this class,we just print all tables we have meet, you can ask a auth server to get real control. Here
are the log info:

```
[owner] [william] auth 
MLSQLTable(None,Some(`/tmp/abc`),TableTypeMeta(hdfs,Set(parquet, json, csv))),
MLSQLTable(None,Some(newtable),TableTypeMeta(temp,Set(temp))),
MLSQLTable(Some(default),Some(abc),TableTypeMeta(hive,Set(hive))),
MLSQLTable(None,Some(cool),TableTypeMeta(temp,Set(temp)))
```

As we can see,there are one hive table named `abc`,two temp tables named `newtable`,`cool`, and one HDFS table named '/tmp/abc'.
You can send these information to the auth server and decide weather to execute the script.

Notice: Make sure you have  skipAuth=false seted when request StreamingPro HTTP API. Auth is disabled by default.

 
 
