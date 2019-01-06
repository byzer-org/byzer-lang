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

RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-4.5.11-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh && \
    /opt/conda/bin/conda clean -tipsy && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc

ENV TINI_VERSION v0.16.1
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini

RUN conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/free/
RUN conda config --add channels https://mirrors.tuna.tsinghua.edu.cn/anaconda/pkgs/main/
RUN conda config --set show_channel_urls yes

RUN mkdir ~/.pip

RUN echo -e "[global]\n trusted-host = mirrors.aliyun.com\n index-url = https://mirrors.aliyun.com/pypi/simple" > ~/.pip/pip.conf

ARG MAVEN_VERSION=3.6.0
ARG USER_HOME_DIR="/root"
ARG SHA=fae9c12b570c3ba18116a4e26ea524b29f7279c17cbaadc3326ca72927368924d9131d11b9e851b8dc9162228b6fdea955446be41207a5cfc61283dd8a561d2f
ARG BASE_URL=https://apache.osuosl.org/maven/maven-3/${MAVEN_VERSION}/binaries

RUN mkdir -p /usr/share/maven /usr/share/maven/ref \
  && curl -fsSL -o /tmp/apache-maven.tar.gz ${BASE_URL}/apache-maven-${MAVEN_VERSION}-bin.tar.gz \
  && echo "${SHA}  /tmp/apache-maven.tar.gz" | sha512sum -c - \
  && tar -xzf /tmp/apache-maven.tar.gz -C /usr/share/maven --strip-components=1 \
  && rm -f /tmp/apache-maven.tar.gz \
  && ln -s /usr/share/maven/bin/mvn /usr/bin/mvn

ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG "$USER_HOME_DIR/.m2"


# download spark binary package from mirror
ENV SPARK_VERSION=2.3.2
ENV URL_BASE http://www.apache.org/dyn/closer.cgi/
ENV FILENAME spark-${SPARK_VERSION}-bin-hadoop2.7.tgz
ENV URL_DIRECTORIES spark/spark-${SPARK_VERSION}/

# use the closer.cgi to pick a mirror
RUN CURLCMD="curl -s -L ${URL_BASE}${URL_DIRECTORIES}${FILENAME}?as_json=1" && \
    BASE=$(${CURLCMD} | grep preferred | awk '{print $NF}' | sed 's/\"//g')  && \
    URL="${BASE}${URL_DIRECTORIES}${FILENAME}" && \
    mkdir /work && \
    curl -o "/work/${FILENAME}" -L "${URL}" && \
    cd /work && tar zxf ${FILENAME} && \
    rm ${FILENAME}

ENV SPARK_HOME /work/spark-${SPARK_VERSION}-bin-hadoop2.7

RUN mkdir -p /home/deploy
RUN mkdir -p /home/deploy/mlsql
RUN mkdir -p /home/deploy/mlsql-console

RUN mkdir -p /home/deploy/mlsql/libs
ENV MLSQL_DISTRIBUTIOIN_URL="streamingpro-mlsql-spark_2.3-1.1.7-SNAPSHOT.jar"
ADD  ${MLSQL_DISTRIBUTIOIN_URL} /home/deploy/mlsql/libs
ADD  start-local.sh /home/deploy/mlsql
WORKDIR /home/deploy/mlsql

#ENTRYPOINT [ "/usr/bin/tini", "--" ]
CMD ./start-local.sh
