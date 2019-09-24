# MLSQL Cluster Routing strategy
                

## Preface
MLSQL Cluster has MLSQL Engine instance management functions, load balancing and multi-service segmentation functions.


## Load balancing

MLSQL Cluster and MLSQL Engine have a fully consistent "/run/script" interface, including parameters.


> How to view the detailed parameters of this interface and What interfaces does MLSQL Cluster contain?
See [MLSQL Beginner Frequent Questions (Continuous Updates)] (https://www.jianshu.com/p/3adbf19bec65) [Where are the HTTP interface documents for the three components of MLSQL]
We can also put multiple instances on the same label to achieve load balancing.


                                                           
Two parameters are added on the basis of MLSQL Engine.


```
tags 
proxyStrategy
```

Tags parameters control which engine to access.
proxyStrategy parameter controls how to access these Engines.
The optional parameters of proxyStrategy are as follows:



1.  ResourceAwareStrategy   Requests are allocated to Engine with the largest remaining resources
                          
2. JobNumAwareStrategy   Request are assigned to the Engine with the least number of tasks
                         
3.  AllBackendsStrategy     Requests are allocated to all Engines
                            
The default is the ResourceAwareStrategy policy.


A simple request example is as follows:

```sql
curl -XPOST http://127.0.0.1:8080 -d 'sql=....& tags=...& proxyStrategy=JobNumAwareStrategy'
``` 