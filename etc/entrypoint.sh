#!/bin/sh
java -Dlogback.configurationFile=/root/logback_deployment.xml -jar /root/hydra.jar --kafka=$Kafka_Broker --db=$Cassandra_Address --keyspace=$Cassandra_Keyspace --user=$Cassandra_Username --pass=$Cassandra_Password  --statSDHost=$StatSD_Host

