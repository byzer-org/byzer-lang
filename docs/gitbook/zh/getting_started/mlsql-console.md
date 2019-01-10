# MLSQL-Console

MLSQL-Console 提供了一个Web控制界面，可对接MLSQL Engine 或者 MLSQL Cluster. Engine或者Cluster都是提供标准的API接口
进行交互，并且缺乏脚本管理功能，MLSQL-Console解决了这些问题。 
界面如下：

![](https://upload-images.jianshu.io/upload_images/1063603-798fb9b3513ff63b.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1000)

主要有几个区组成：

1. 项目脚本管理区
2. 脚本编辑区
3. 信息反馈区
4. 结果展示区
5. 控制台里的项目脚本管理区根目录统一，第二层级必须是项目，之后内部可以创建目录或者脚本。脚本之间可以通过include互相引用使用。