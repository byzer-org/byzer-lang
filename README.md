# StreamingPro

This document is for StreamingPro developers. User's manual is now on its way.

## Introduction

StreamingPro is not a complete
application, but rather a code library and API that can easily be used
to build your streaming application which may run on Spark Streaming.

StreamingPro also make it possible that all you should do to build streaming program is assembling components(eg. SQL Component) in configuration file. 
Of source , if you are a geek who like do every thing by programing,we also encourage you use API provided
by StreamingPro which is more easy to use then original API designed by Spark/Storm.

## Features

* Setup job flows with configuration
* Supports Add/Update/Remove job flows dynamically at runtime via Rest API 
* Brand new API to make program modularized 
* Support for job flows building by writing SQL  

Notes: 

Feature 2  is available only when Spark Streaming receives data from 
Kafka using  Direct Approach (No Receivers) mode.


* [Properties](https://github.com/allwefantasy/streamingpro/wiki/User's-manual)
* [Setup-Project](https://github.com/allwefantasy/streamingpro/wiki/Setup-Project)
* [Run Your First Application](https://github.com/allwefantasy/streamingpro/wiki/Run-your-first-application)
* [Dynamically add Job via Rest API](https://github.com/allwefantasy/streamingpro/wiki/Dynamically-add-Job-via-Rest-API)
* [How To Add New Compositor](https://github.com/allwefantasy/streamingpro/wiki/How-To-Add-New-Compositor)



## Add other Runtime support 

For now, StreamingPro can run on Spark Streaming. If you want it runs on other
 platform like Storm/Fink, you can do something follow: 

```
 class StormRuntime extends StreamingRuntime with PlatformManagerListener  
```









 