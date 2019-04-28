# Resource Management

When you have deployed MLSQL Stack in production, then there are so many 
scenarios driving you to deploy bunch of MLSQL Engines: for data exploration,
Machine learning, Load balance,ETL engine,Stream Engine,API server and more.

These will bring much resource pressure on MLSQL Stack service provider. For data exploration,
when people start to work in morning and they need  MLSQL Engine to execute their script
as quick as possible, this means more resource required, and when people are off duty,
the corresponding MLSQL Engine will just dry run. At the same time, the ETL jobs are mostly run
at night. So if there is a way we can achieve user-defined resource adjust strategy, it would be awesome.

For example, you can set a timer which will add resources at morning and reduce resources at night.MLSQL Engine
supports dynamically adjust resource at runtime.   
