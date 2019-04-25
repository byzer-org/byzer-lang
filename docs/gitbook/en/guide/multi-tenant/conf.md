# How to configure multi-tenant

Multi-tenant is enabled by http request parameters. For example, if you want to run script, you should post `/run/script` 
endpoint. 

```bash
curl -XPOST `http://127.0.0.1:9003/run/script` -d '
sql=      content of script
owner=    login user
jobType=  script
jobName=  by default is uuid
timeout=  
silence=  weather return result
sessionPerUser= true/fase enable multi-tenant if set true 
async=    true/false 
callback= when async set true, you should give a callback url
skipInclude= true/false 
skipAuth=  
'
```

When you set sessionPerUser true, then you have enabled  multi-tenant mode. Please make sure you also have set the `owner`.

You also can logout the user like this:

```bash
http://127.0.0.1:9003/run/script?owner=&sessionPerUser=true
``` 


         