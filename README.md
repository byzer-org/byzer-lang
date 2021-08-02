## MLSQL

MLSQL is a Programming Language designed For Big Data and AI, it also has a distributed runtime.

```sql
load hive.`raw.stripe_discounts` as discounts;
load hive.`raw.stripe_invoice_items` as invoice_items;

select
        invoice_items.*,
        case
            when discounts.discount_type = 'percent'
                then amount * (1.0 - discounts.discount_value::float / 100)
            else amount - discounts.discount_value
        end as discounted_amount

    from invoice_items

    left outer join discounts
        on invoice_items.customer_id = discounts.customer_id
        and invoice_items.invoice_date > discounts.discount_start
        and (invoice_items.invoice_date < discounts.discount_end
             or discounts.discount_end is null)
as joined;



select

        id,
        invoice_id,
        customer_id,
        coalesce(discounted_amount, amount) as discounted_amount,
        currency,
        description,
        created_at,
        deleted_at

    from joined
as final;

select * from final as output;
```

## Official WebSite

[https://mlsql.ai](https://mlsql.ai)

Find more examples on:

1. [中文文档](http://docs.mlsql.tech/mlsql-stack/)
2. [Video](https://space.bilibili.com/22610047)

## Docker

### Pulling Sandbox Docker Image

For Spark 2.4.3 bundle:

```shell
docker pull techmlsql/mlsql-sandbox:2.4.3-2.1.0-SNAPSHOT
```

For Spark 3.1.1 bundle：

```shell
docker pull techmlsql/mlsql-sandbox:3.1.1-2.1.0-SNAPSHOT
```

### Start Container


```
docker run -d \
-p 3306:3306 \
-p 9002:9002 \
-e MYSQL_ROOT_PASSWORD=mlsql \
--name mlsql-sandbox-2.4.3-2.1.0-SNAPSHOT \
mlsql-sandbox:2.4.3-2.1.0-SNAPSHOT
```



## <a id="Download"></a>Download MLSQL
* The latest stable version is v2.0.1; snapshot version is 2.1.0-SNAPSHOT
* You can download from [MLSQL Website](http://download.mlsql.tech)
* Spark 2.4.3/3.1.1 are tested

***Naming Convention***

mlsql-engine_${spark_major_version}-${mlsql_version}.tgz
```shell
## Pre-built for Spark 2.4.3
mlsql-engine_2.4-2.1.0-SNAPSHOT.tar.gz 

## Pre-built for Spark 3.1.1
mlsql-engine_3.0-2.1.0-SNAPSHOT.tar.gz  
```  

## <a id="Build"></a>Building a Distribution
### Prerequisites
- JDK 8+
- Maven
- Linux or MacOS

### Downloading Source Code
```shell
## Clone the code base
git clone https://github.com/allwefantasy/mlsql.git .
cd mlsql
```

### Building Spark 2.4.3 Bundle
```shell
export MLSQL_SPARK_VERSION=2.4
./dev/make-distribution.sh
```

### Building Spark 3.1.1 Bundle
```shell
export MLSQL_SPARK_VERSION=3.0
./dev/make-distribution.sh
```
### Building without Chinese Analyzer
```shell
## Chinese analyzer is enabled by default.
export ENABLE_CHINESE_ANALYZER=false
./dev/make-distribution.sh <spark_version>
```
### Building with Aliyun OSS Support
```shell
## Aliyun OSS support is disabled by default
export OSS_ENABLE=true
./dev/make-distribution.sh <spark_version>
```

## Deploying
1. [Download](#Download) or [build a distribution](#Build) 
2. Install Spark and set environment variable SPARK_HOME, make sure Spark version matches that of MLSQL
3. Deploy tgz
- Set environment variable MLSQL_HOME
- Copy distribution tar ball over and untar it

4.Start MLSQL in local mode
```shell
cd $MLSQL_HOME
## Run process in background
nohup ./bin/start-local.sh 2>&1 > ./local_mlsql.log &
```
5. Open a browser and type in http://localhost:9003, have fun.

Directory structure
```shell
|-- mlsql
    |-- bin        
    |-- conf       
    |-- data       
    |-- examples   
    |-- libs       
    |-- README.md  
    |-- LICENSE
    |-- RELEASE
```

## Contributing to MLSQL

If you are planning to contribute to this repository, please create an issue at [our Issue page](https://github.com/allwefantasy/streamingpro/issues)
even if the topic is not related to source code itself (e.g., documentation, new idea and proposal).

This is an active open source project for everyone,
and we are always open to people who want to use this system or contribute to it.


## Contributors

* Zhu William/allwefantasy#gmail.com
* Chen Fu/cfmcgrady#gmail.com
* Geng Yifei/pigeongeng#gmail.com
* wanp1989/wanp1989#126.com
* chenliang613
* RebieKong
* CoderOverflow
* ghsuzzy
* xubo245
* zhuohuwu0603
* liyubin117
* 9bbword
* Slash-Wong/523935329#qq.com
* anan0120/158989903#qq.com


##  WeChat Group

扫码添加K小助微信号，添加成功后，发送  mlsql  这5个英文字母进群。

![](https://github.com/allwefantasy/mlsql/blob/master/images/dc0f4493-570f-4660-ab41-0e487b17a517.png)

