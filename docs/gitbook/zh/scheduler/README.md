# 如何集成调度

MLSQL内置了一个调度引擎（1.5.0及以上可以用）。当然，用户也可以使用譬如第三方调度如[Easy Scheduler](https://analysys.github.io/easyscheduler_docs_cn/),
然后在它提供的交互界面设置你的定时任务。

## 整合方案

MLSQL的分布式运行环境 MLSQL Engine提供了http接口供用户传递脚本调用。所以比较直观的做法如下：

1. 通过shell脚本，比如使用CURL来获取脚本内容，并且提交给MLSQL Engine.
2. 开发一个Java Proxy,可以通过该Proxy获取脚本内容并且通过该Proxy将内容提交给MLSQL Engine执行。

## 异步还是同步？

当你使用shell或者Java Proxy的时候，任务提交给Engine的时候，应该是异步还是同步呢？我们建议同步，
并且设置一定的超时时间。这样，调度程序可以检查到必要的错误。

下一节，我们会讲解如何使用诶之调度器来完成一些日常的调度工作。
