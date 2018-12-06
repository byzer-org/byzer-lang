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

1. Start up MLSQL instances.
2. Setup DB.Find the db.sql in resource directory of streamingpro-cluster module and create database called streamingpro_cluster then execute the db.sql.
3. Build streamingpro-cluster 

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
# proxyStrategy=ResourceAwareStrategy|JobNumAwareStrategy
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
     override def invoke(backends: Seq[BackendCache]): Option[BackendCache] = {
   
       val tagSet = tags.split(",").toSet
       //get all backends(MLSQL instances meta)
       var backends = BackendService.backends
       // filter by tags
       if (!tags.isEmpty) {
         backends = backends.filter(f => tagSet.intersect(f.meta.getTag.split(",").toSet).size > 0)
       }
       // Get current available free cpus of all MLSQL instances
       val backend = backends.seq.map { b =>
         val res = b.instance.instanceResource
         val resource = res.toBean[CSparkInstanceResource]().head
         (resource.totalCores - resource.totalTasks, b)
       }.sortBy(f => f._1).reverse.headOption.map(f => f._2.meta)
   
       // return the final choosed backend.   
       BackendService.find(backend)
     }
   }
```


     