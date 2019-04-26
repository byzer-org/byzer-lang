#  Build MLSQL Stack Manually


## MLSQL-Engine

1. clone 

```
git clone git@github.com:allwefantasy/streamingpro.git
```

2. compile

```
cd streamingpro
export MLSQL_SPARK_VERSION=2.4
./dev/package.sh
```

After the compiling, you will find a jar in this place:

```
streamingpro-mlsql/target/streamingpro-mlsql-spark_2.4-1.2.0-SNAPSHOT.jar
```

The size of jar is almost 200+m. Now, create a release:

```
export VERSION=1.2.0-SNAPSHOT
mkdir -p /tmp/mlsql-server/libs
cp  streamingpro-mlsql/target/streamingpro-mlsql-spark_2.4-${VERSION}.jar /tmp/mlsql-server/libs
cp dev/start-local.sh /tmp/mlsql-server
cd /tmp/mlsql-server

export SPARK_HOME=~/Softwares/spark-2.4.0-bin-hadoop2.7
./start-local.sh
```

Done！Now you can access `127.0.0.1:9003`; 


## MLSQL Cluster

Since cluster and engine are in the same project. So just use the directory 
cloned in preview steps:

```
cd streamingpro
export MLSQL_CLUSTER_VERSION=${MLSQL_CLUSTER_VERSION:-1.2.0-SNAPSHOT}
mvn -DskipTests -Pcluster-shade -am -pl streamingpro-cluster clean package
cd streamingpro-cluster/
```

After compiling, you will found jar file like this：

```
export VERSION=1.2.0-SNAPSHOT
target/streamingpro-cluster-${VERSION}.jar
```


Cluster/Console are both depends MySQL.  We should create mysql db according to the file in 
 `src/main/resources/db.sql `. Suppose the db name is mlsql_cluster.

Now, create a release：

```
# make sure you are in  streamingpro-cluster
#cd streamingpro-cluster/
export VERSION=1.2.0-SNAPSHOT
mkdir -p /tmp/mlsql-cluster/
cp  target/streamingpro-cluster-${VERSION}.jar /tmp/mlsql-cluster/
cp dev/mlsql-cluster-docker/start.sh  /tmp/mlsql-cluster

##modify the mysql configuration in  application.docker.yml, e.g. db name and password 
cp dev/mlsql-cluster-docker/application.docker.yml  /tmp/mlsql-cluster
cd  /tmp/mlsql-cluster

export MLSQL_CLUSTER_CONFIG_FILE=application.docker.yml
export MLSQL_CLUSTER_JAR=streamingpro-cluster-${VERSION}.jar
./start.sh
```

Try to visit `127.0.0.1:8080` to check it's ok.


## MLSQL-Console

1. Clone 

```
git clone  git@github.com:allwefantasy/mlsql-api-console.git
```

2. Compile

```
mvn clean package -Pshade
```

Again, check the jar is whether exists:

```
export VERSION=1.2.0-SNAPSHOT
target/mlsql-api-console-${VERSION}.jar
```

3.We should create mysql db according to the file in 
 `src/main/resources/db.sql `. Suppose the db name is mlsql_console.

4. Create a release:

```
export VERSION=1.2.0-SNAPSHOT
cd mlsql-api-console
mkdir -p /tmp/mlsql-console/
cp target/mlsql-api-console-${VERSION}.jar /tmp/mlsql-console/
cp  dev/docker/start.sh  /tmp/mlsql-console/
## change the configuration of db e.g. address,dbname, password
cp dev/docker/application.docker.yml   /tmp/mlsql-console/

cd /tmp/mlsql-console/

export MLSQL_CONSOLE_JAR="mlsql-api-console-${VERSION}.jar"
export MLSQL_CLUSTER_URL=http://127.0.0.1:8080
export MY_URL=http://127.0.0.1:9002
## If you are using macos, please use path like /Users/mlsql
export USER_HOME=/home/users 
## disable auth
export ENABLE_AUTH_CENTER=false 
export MLSQL_CONSOLE_CONFIG_FILE=application.docker.yml
./start.sh
```

Now try to visit `127.0.0.1:9002` to check the installation is ok.

## If You want to compile the React project used in MLSQL Console

1. clone 

```
git clone  git@github.com:allwefantasy/mlsql-web-console.git
```

2. install and build

```
npm install
npm run build
```

3. Copy all the built files to MLSQL Console

```
rm -rf ${MLSQL_CONSOLE_HOME}/src/main/resources/streamingpro/assets/*
cp -r build/* ${MLSQL_CONSOLE_HOME}/src/main/resources/streamingpro/assets/*
```

## Download from Pre-Build releases

Go to these website： [http://download.mlsql.tech](http://download.mlsql.tech/)
