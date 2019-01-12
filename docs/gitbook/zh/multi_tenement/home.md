# 租户主目录设置

无论是否开启多租户（也就是sessionPerUser=true）,我们都可以针对每个用户设置一个主目录。和多租户一样，控制租户主目录
的设置也是在http请求的时候进行设置。主要参数有：

```
allPathPrefix={"jack":"/home/jack","back":"/home/back"}
defaultPathPrefix=/home/all
```
allPathPrefix是json格式，key为用户民个，value位对应的主目录。当一个用户没有主目录是，则使用defaultPathPrefix
的值。

主目录有什么用呢？在load,save,train/run时涉及到的一些目录，会被自动补全主目录。比如

```sql
load text.`/tmp/wow` as table1;
```

这个时候，load的实际目录是 "/home/jack/tmp/wow".

同样的比如

```sql
load table1 as parquet.`tmp/jack2`;
```

这个时候 实际目录会是：/home/jack/tmp/jack2

对于有些参数里涉及到目录的，可以这么用：

```sql
run data as Example.`tmp/model` where path="${HOME}/jack/a.dic";

```

比如这个例子，我们在path里写了一个路径，系统不会自动加上主目录，用户可以通过`${HOME}`来做替换。
