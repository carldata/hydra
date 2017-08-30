FROM openjdk:jdk-alpine

ENV SCALA_VERSION 2.12.3
ENV Kafka_Broker localhost:9092
ENV Cassandra_Address localhost
ENV Cassandra_Keyspace default

WORKDIR /root
ADD target/scala-2.12/hydra.jar /root/hydra.jar
ADD entrypoint.sh /root/entrypoint.sh
ENTRYPOINT ["/bin/sh","/root/entrypoint.sh"]

