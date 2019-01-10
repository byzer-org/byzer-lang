# 如何发布更新

> 我们以更新英文文档为例.

1.`fork`文档项目[documents-en](https://github.com/mlsql-project/documents-en)，并为`streamingpro`工程添加远程仓库.

```
# 注意添加远程仓库是为streamingpro工程添加的
# git仓库地址替换为自己fork的地址
git remote add fchen-doc-en git@github.com:cfmcgrady/documents-en.git
```

> `fchen-doc-en`可以修改为自己方便识别的名字

2.推送更新的文档HTML到fork的代码库.

```
# fchen-doc-en为上面我们添加的远程仓库，master为远程的分枝名
git subtree push --prefix=docs/gitbook/en/_book fchen-doc-en master
```

> 注意，该命令需要在`streamingpro`工程的跟目录下执行.

3.发布更新.

接下来就是走正常的pr流程了，将自己fork仓库的`master`分枝合并到[documents-en](https://github.com/mlsql-project/documents-en)的`gh-pages`分枝

