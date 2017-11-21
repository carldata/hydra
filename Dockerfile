FROM openjdk:jdk-alpine

ENV SCALA_VERSION 2.12.3
ENV KAFKA_BROKER  localhost:9092
ENV CASSANDRA_ADDRESS localhost
ENV CASSANDRA_KEYSPACE default
ENV CASSANDRA_PASSWORD default
ENV CASSANDRA_USERNAME default
ENV STATSD_HOST localhost

WORKDIR /root
ADD target/scala-2.12/hydra.jar /root/hydra.jar
ADD etc/gelf.xml /root/gelf.xml
ADD etc/entrypoint.sh /root/entrypoint.sh
ENTRYPOINT ["/bin/sh","/root/entrypoint.sh"]

