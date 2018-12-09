## Cluster Support

Once you have developed MLSQL, then you will find more and more MLSQL instances are deployed.
So how to manager these MLSQL instances would be a big problem.

MLSQL-Cluster is designed to resolve this issue.

The following picture shows where  the MSLQL-Cluster is and what it is:  

![](https://github.com/allwefantasy/streamingpro/raw/master/images/WX20181205-105228@2x.png)

MLSQL-Cluster should have following features:

1. Dynamic resource adjust. Proxy will close the MLSQL instances with specific tag tagged if the system load is low or create
new MLSQL instances if the system load increase. Notice that MLSQL instance  supports worker dynamic allocation, they are different.

2. Dispatching. We may have different businesses, and each of them need multi MLSQL instances as load balance. The first step is MLSQL-Cluster
will dispatch the requests to the proper instances according to the tags and then dispatch the single request to specific instance by some strategy e.g.
resource-aware-strategy or tasks-aware-strategy.

## Setup MLSQL-Cluster

1. Start MLSQL instances.
2. Setup DB. Find the `db.sql` in resource directory of streamingpro-cluster module and create database called streamingpro_cluster then execute the db.sql.
3. Build streamingpro-cluster package

```
mvn -Pcluster-shade -am -pl streamingpro-cluster clean package
```

4. Start server

```
java -cp .:streamingpro-cluster-1.1.6-SNAPSHOT.jar tech.mlsql.cluster.ProxyApplication -config application.yml
``` 

No you can use postman or CURL to add MLSQL instances information to  streamingpro-cluster.

```
# name=backend1
# url=127.0.0.1:9003
# tag=read,write
curl -XPOST http://127.0.0.1:8080/backend/add -d 'name=backend1&url=127.0.0.1%3A9003&tag=read%2Cwrite'
```

you can use this api to check the list of backends:

```
curl -XGET http://127.0.0.1:8080/backend/list
```

try to run MLSQL script:

```
# sql=select sleep(1000) as a as t;
# tags=read
# proxyStrategy=ResourceAwareStrategy|JobNumAwareStrategy|AllBackendsStrategy
curl -X POST \
  http://127.0.0.1:8080/run/script \  
  -H 'content-type: application/x-www-form-urlencoded' \  
  -d 'sql=select%20sleep(100000)%20as%20a%20as%20t%3B&tags=read'
```

Done.

## How to create your owner  strategy

Here is the implementation of FreeCoreBackendStrategy:

```scala
  class ResourceAwareStrategy(tags: String) extends BackendStrategy {
    override def invoke(backends: Seq[BackendCache]): Option[Seq[BackendCache]] = {
      val tagSet = tags.split(",").toSet
      var nonActiveBackend = BackendService.nonActiveBackend
      if (!tags.isEmpty) {
        nonActiveBackend = nonActiveBackend.filter(f => tagSet.intersect(f.getTag.split(",").toSet).size > 0)
      }
      val backend = if (nonActiveBackend.size > 0) {
        nonActiveBackend.headOption
      } else {
        var activeBackends = BackendService.activeBackend.toSeq
        if (!tags.isEmpty) {
          activeBackends = activeBackends.filter(f => tagSet.intersect(f._1.getTag.split(",").toSet).size > 0).sortBy(f => f._2)
        }
        activeBackends.headOption.map(f => f._1)
  
      }
      BackendService.find(backend).map(f => Seq(f))
    }
  }
```

## JobNumAwareAllocateStrategy

As we mentioned before, mlsql-cluster supports dynamic resource adjust. For now, mlsql local/yarn-client modes
are available.  JobNumAwareAllocateStrategy is the only allocate strategy implemented.

In order to enable JobNumAwareAllocateStrategy, please use the api `/monitor/add` to add 
a monitor which will tell mlsql-cluster who(tags) should be monitored and use the api `/ecs/add`
to tell mlsql-cluster where is the free elastic server.

monitor parameters example:

```
          "name" -> "jack-monitor",
          "tag" -> "jack",
          "minInstances" -> "1",
          "maxInstances" -> "3",
          "allocateType" -> "local",
          "allocateStrategy" -> "JobNumAwareAllocateStrategy"

``` 

The monitor will make sure the number of instances with tag `jack` between 1-3 using JobNumAwareAllocateStrategy Strategy 
to allocate new instance.   
 


ecs parameters example:

```
    "ip" -> "127.0.0.1",
    "keyPath" -> "./ssh/private-key",
    "loginUser" -> "root",
    "name" -> "backend2",
    "sparkHome" -> "/home/spark",
    "mlsqlHome" -> "/home/mlsql",
    "mlsqlConfig" ->
      """
        |{"master":"local",
        |"name":"mlsql",
        |"conf":"spark.serializer=org.apache.spark.serializer.KryoSerializer",
        |"streaming.name":"mlsql",
        |"streaming.driver.port":"9003",
        |"streaming.spark.service":"true",
        |"streaming.platform":"spark"
        |}
      """.stripMargin,
    "executeUser" -> "webuser",
    "tag" -> "jack"

```

ecs tell mlsql-cluster how to start a mlsql-instance. With these information, mlsql-cluster 
can ssh to server whose ip is 127.0.0.1 and start a local mode spark mlsql instance listening port 9003.
The mlsql-cluster  will be updat this row to in_use and add a new backen to backend table.  
  


     