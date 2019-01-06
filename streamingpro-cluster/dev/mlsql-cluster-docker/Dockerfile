FROM debian:latest

#  $ docker build . -t continuumio/miniconda3:latest -t continuumio/miniconda3:4.5.11
#  $ docker run --rm -it continuumio/miniconda3:latest /bin/bash
#  $ docker push continuumio/miniconda3:latest
#  $ docker push continuumio/miniconda3:4.5.11

ENV LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH /opt/conda/bin:$PATH

RUN apt-get update --fix-missing && \
    apt-get install -y openjdk-8-jre-headless wget bzip2 ca-certificates curl git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/deploy
RUN mkdir -p /home/deploy/mlsql-cluster


WORKDIR /home/deploy/mlsql-cluster

ENV MLSQL_CLUSTER_JAR=streamingpro-cluster-1.1.7-SNAPSHOT.jar
ENV MLSQL_CLUSTER_CONFIG_FILE="application.docker.yml"
ADD ${MLSQL_CLUSTER_JAR} /home/deploy/mlsql-cluster/
ADD ${MLSQL_CLUSTER_CONFIG_FILE} /home/deploy/mlsql-cluster/
ADD start.sh /home/deploy/mlsql-cluster/

ENV MYSQL_HOST=mlsql-console-mysql
RUN sed -i "s/MYSQL_HOST/${MYSQL_HOST}/g" application.docker.yml

CMD ./start.sh
