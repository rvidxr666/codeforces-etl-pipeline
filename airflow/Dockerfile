FROM apache/airflow:2.10.3

ENV AIRFLOW_HOME=/opt/airflow

USER root
RUN apt-get -y update -qq && apt-get -y install vim -qqq -y &&\
    apt-get -y install wget tar

# git gcc g++ -qqq

ENV SPARK_INSTALL="/usr/local/lib/spark"
ENV JAVA_INSTALL="/usr/local/lib/java"

# Installing java
RUN mkdir -p ${JAVA_INSTALL} && cd ${JAVA_INSTALL} &&\
    wget https://download.oracle.com/java/17/archive/jdk-17_linux-x64_bin.tar.gz &&\
    tar xzfv jdk-17_linux-x64_bin.tar.gz &&\
    rm jdk-17_linux-x64_bin.tar.gz

ENV JAVA_HOME=${JAVA_INSTALL}/jdk-17

# Installing Spark
RUN mkdir -p ${SPARK_INSTALL} && cd ${SPARK_INSTALL} &&\
    wget https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar xzfv spark-3.4.0-bin-hadoop3.tgz && \
    rm spark-3.4.0-bin-hadoop3.tgz

ENV SPARK_HOME=${SPARK_INSTALL}/spark-3.4.0-bin-hadoop3
ENV PATH=${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}

RUN cd ${HOME} &&\
    wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-j_8.0.33-1debian11_all.deb && \
    apt install ./mysql-connector-j_8.0.33-1debian11_all.deb &&\
    cp /usr/share/java/* /usr/local/lib/spark/spark-3.4.0-bin-hadoop3/jars

RUN pyspark --help

ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/build:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip:$PYTHONPATH

USER $AIRFLOW_UID
RUN pip3 install py4j pymysql