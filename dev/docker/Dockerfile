FROM ubuntu:18.04

RUN apt-get update && apt-get install -y locales && rm -rf /var/lib/apt/lists/* \
    && localedef -i en_US -c -f UTF-8 -A /usr/share/locale/locale.alias en_US.UTF-8
ENV LANG=C.UTF-8 LC_ALL=C.UTF-8

RUN apt-get update && apt-get install -y --no-install-recommends apt-utils build-essential software-properties-common curl wget unzip nano git
RUN add-apt-repository -y ppa:openjdk-r/ppa
RUN apt-get update
RUN apt-get -y install openjdk-8-jdk

ENV PATH /opt/conda/bin:$PATH

# RUN apt-get update --fix-missing && \
#     apt-get install -y openjdk-8-jre-headless wget bzip2 ca-certificates curl git && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/*

RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-4.5.11-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh && \
    /opt/conda/bin/conda clean -tipsy && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> /etc/profile && \
    echo "conda activate base" >> /etc/profile && \
    . /etc/profile


RUN mkdir ~/.pip
RUN echo "[global]\n trusted-host = mirrors.aliyun.com\n index-url = https://mirrors.aliyun.com/pypi/simple" > ~/.pip/pip.conf

RUN conda create --name dev python=3.6 --yes --quiet
RUN  . /etc/profile && conda activate dev && pip install --no-cache-dir \
Cython numpy pandas scikit-learn plotly pyarrow==0.10.0 ray==0.8.0 \
aiohttp psutil setproctitle grpcio \
watchdog requests click uuid sfcli  pyjava


ARG SPARK_VERSION
ARG MLSQL_SPARK_VERSION
ARG MLSQL_VERSION
ARG SCALA_VERSION

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
RUN mkdir -p /tmp/__mlsql__/logs

RUN mkdir -p /home/deploy/mlsql/libs
ENV MLSQL_DISTRIBUTIOIN_URL="streamingpro-mlsql-spark_${MLSQL_SPARK_VERSION}_${SCALA_VERSION}-${MLSQL_VERSION}.jar"
ADD  ${MLSQL_DISTRIBUTIOIN_URL} /home/deploy/mlsql/libs
ADD  lib/ansj_seg-5.1.6.jar /home/deploy/mlsql/libs
ADD  lib/nlp-lang-1.7.8.jar /home/deploy/mlsql/libs
ADD  start-local.sh /home/deploy/mlsql
ADD  log4j.properties ${SPARK_HOME}/conf
WORKDIR /home/deploy/mlsql

#ENTRYPOINT [ "/usr/bin/tini", "--" ]
CMD ./start-local.sh
