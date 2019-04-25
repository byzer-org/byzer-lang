# User Home

No matter multi-tenant is enabled, every owner should have their owen home directory, just like linux system.
Again,the home directory is configured by HTTP request parameters:


```bash
curl -XPOST `http://127.0.0.1:9003/run/script` -d '
defaultPathPrefix=/home/users/jack
owner=jack
'
```

So what does this do? when you use load/save/train/run, you many operate hdfs/local file system. This make sure you 
only works with you own home. For example:

```sql
load text.`/tmp/wow` as table1;
```

the actual path is `/home/users/jack/tmp/wow`.


  
