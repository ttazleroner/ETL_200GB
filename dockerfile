FROM apache/airflow:2.10.2

USER root

RUN sed -i 's/deb.debian.org/mirror.mephi.ru/g' /etc/apt/sources.list.d/debian.sources || \
    sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

USER airflow

RUN pip install --no-cache-dir apache-airflow-providers-apache-spark pyspark kafka-python clickhouse-connect