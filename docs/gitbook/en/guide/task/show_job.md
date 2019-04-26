# Show running jobs

You can use command like this:

```sql
!show jobs;
``` 

it will display all running jobs belongs to you:

```
owner                   jobType jobName                                 jobContent                      groupId                  startTime       timeout
----------------------------------------------------------------------------------------------------
allwefantasy@gmail.com    script    2595b404-c54c-436a-9cb7-ccd33438bc36    load _mlsql_.`jobs` as output;    23                         1547194894263    -1

```

Get the detail of some job with this:

```sql
-- 2595b404-c54c-436a-9cb7-ccd33438bc36 is jobName or groupId 
!show "job/2595b404-c54c-436a-9cb7-ccd33438bc36";
```

Get the resource usage of current MLSQL Engine: 

```sql
!show resource;
```

Show resource of one job:

```sql
!show "resource/2595b404-c54c-436a-9cb7-ccd33438bc36";
```

>  groupId,jobName both can be the uniq id.

