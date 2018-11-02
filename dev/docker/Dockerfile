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

RUN echo ' [global]\n trusted-host = mirrors.aliyun.com\n index-url = https://mirrors.aliyun.com/pypi/simple' > ~/.pip/pip.conf

# download spark binary package from mirror
ENV URL_BASE http://www.apache.org/dyn/closer.cgi/
ENV FILENAME spark-2.3.2-bin-hadoop2.7.tgz
ENV URL_DIRECTORIES spark/spark-2.3.2/

# use the closer.cgi to pick a mirror
RUN CURLCMD="curl -s -L ${URL_BASE}${URL_DIRECTORIES}${FILENAME}?as_json=1" && \
    BASE=$(${CURLCMD} | grep preferred | awk '{print $NF}' | sed 's/\"//g')  && \
    URL="${BASE}${URL_DIRECTORIES}${FILENAME}" && \
    mkdir /work && \
    curl -o "/work/${FILENAME}" -L "${URL}" && \
    cd /work && tar zxf ${FILENAME} && \
    rm ${FILENAME}

ENV SPARK_HOME /work/spark-2.3.2-bin-hadoop2.7

ENTRYPOINT [ "/usr/bin/tini", "--" ]
CMD [ "/bin/bash" ]
