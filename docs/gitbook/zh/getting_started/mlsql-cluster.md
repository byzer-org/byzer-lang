# MLSQL-Cluster

随着MLSQL Engine部署的实例愈发的增多，有给各条业务线部署的MLSQL Engine group,也有给算法组，
研发组等等部署的单独MLSQL Engine group. 我们希望所有这些MLSQL Engine能够被：

1. 统一的管理
2. 组内的负载均衡
3. 不同组之间互相借用资源
4. 同组内的MLSQL 实例数动态调整

MLSQL-Cluster 实现了相关功能。架构图如下：

![](https://upload-images.jianshu.io/upload_images/1063603-3631f9176c3d9703.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

