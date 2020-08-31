# 离线安装插件

> 2.0.1-SNAPSHOT、2.0.1 及以上版本支持。

考虑到很多场景，我们需要引擎启动的时候就具备某个插件的功能，亦或是我们没办法访问外部网络，这个时候就可以通过离线方式安装插件。

## 下载Jar包并且上传到你的服务器
首先，手动下载相应的Jar包(我这里用一段python代码下载)

```python
targetPath = "./__mlsql__/mlsql-excel-2.4_2.11.jar"
with requests.get("http://store.mlsql.tech/run",params={
                  "action":"downloadPlugin",
                  "pluginType":'''MLSQL_PLUGIN''',
                  "pluginName": "mlsql-excel-2.4",
                  "version":"0.1.0-SNAPSHOT"
              }, stream=True) as r:
    
    r.raise_for_status()
    downloadSize = 0
    with open(targetPath,"wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            downloadSize  += 8192
            print(str(downloadSize/1024)+"\n")
            f.write(chunk)
```

值得注意的是，在上面的参数中，唯一需要根据场景修改的是pluginName和version.

## 启动时配置jar包以及启动类

在启动脚本里，配置插件主类(这里以Excel插件安装为例)：

```
-streaming.plugin.clzznames tech.mlsql.plugins.ds.MLSQLExcelApp
```

同时使用 `--jars` 将我们上一个步骤的jar包带上。 这个时候你就可以使用excel数据源了。
