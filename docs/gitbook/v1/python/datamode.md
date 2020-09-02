# dataMode 详解

在使用!ray命令时，dataMode是必须设置的。dataMode可选值为 data/model. 那么他们到底
是什么含义呢？

## data

如果你使用了foreach/map_iter 等高阶函数，则使用data模式。

## model

其他情况使用model即可。
