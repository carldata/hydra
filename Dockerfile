FROM openjdk:jdk-alpine

ENV SCALA_VERSION 2.12.3
ENV Kafka_Broker localhost:9092
ENV Cassandra_Address localhost
ENV Cassandra_Keyspace default
ENV Cassandra_Password default
ENV Cassandra_Username default
ENV StatsD_Host localhost

WORKDIR /root
ADD target/scala-2.12/hydra.jar /root/hydra.jar
ADD etc/gelf.xml /root/gelf.xml
ADD etc/entrypoint.sh /root/entrypoint.sh
ENTRYPOINT ["/bin/sh","/root/entrypoint.sh"]

