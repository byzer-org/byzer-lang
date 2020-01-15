## 如何让 MLSQL Engine 7*24小时稳定运行

MLSQL可以让你的生活变得轻松很多，但是任何一个系统稳定的运行，也是需要花一些时间和精力的。在这个章节里，我们会罗列一些需要注意的事项，
帮助我们去面对实际上线MLSQL之后可能存在的一些不稳定因数。

### Driver僵死

Driver 僵死可能有两个情况：

1. MLSQL Driver存在缓慢的内存泄露，导致程序Full GC 而没有响应。
2. 使用了类似excel之类的比较占用内存的格式亦或者是 MLSQL Driver内存太小，都可能导致GC问题

如果是缓慢的内存泄露，可以查询具体的原因有的放矢解决，也可以定时重启让释放内存得到释放。如果用户采用定期重启机制，需要考虑的是，
如何让用户无感知重启过程。具体做法可以是：

1. 启动一个新的MLSQL Engine实例
2. 将请求执行指向到新的实例
1. 通过!show jobs等命令查询旧的示例当前是不是有任务，如果没有任务，则杀死。

### MLSQL意外退出

各种原因都有可能导致MLSQL Engine意外退出。推荐用户采用yarn-client模式部署，这样可以监控driver进程是否存活，如果不存活，自动拉起即可。

### SparkContext 已经stopped

当Spark无法正确处理错误的时候，SparkContext可能会被关闭，从而无法执行新的任务。用户可以通过各种hook机制，或者监控返回的错误，重启MLSQL实例。


### 把MLSQL Engine实例当做一个普通的Web程序来对待

我们完全可以把MLSQL Engine Driver 放在K8s上跑，然后Executor之类的还是在Yarn上跑。这样MLSQL Engine实例就是普通的K8s web服务，
从而能够享受K8s一系列的基础设施。

### 初始化

connect语句等不少信息，生命周期是和MLSQL Instance一样的。如果发生了重启，这些信息就会丢失，用户需要从新注册。我们既可以安装[connect persist](https://github.com/allwefantasy/mlsql-plugins/tree/master/connect-persist)插件
来保证connect语句得到持久化，也可以自己写一段json脚本，然后在MLSQL启动时配置上，参考[如何执行初始化脚本](http://docs.mlsql.tech/zh/include/init.html)

### 充分利用调度

很多工作都可以用调度来完成，比如我们需要清理一些目录或者库表，我们可以写个MLSQL脚本，然后设置上定时调度即可完成。这也可以应用于对MLSQL Engine的维护。

### 一些可能需要的配套系统

1. 元数据系统
2. 权限校验后端
3. Web Console(包含脚本存储和管理，配合MLSQL Engine实现include功能等，同时可以对外提供web api服务)
4. 调度系统（推荐dolphin scheduler等）

