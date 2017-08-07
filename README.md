# Hydra

[![Build status](https://travis-ci.org/carldata/hydra.svg?branch=master)](https://travis-ci.org/carldata/hydra)

Stream processing engine which executes custom script written in [FlowScript](http://github.com/carldata/flow-script).

Interface to this engine is based on Kafka
 
## Quick start

### Run Kafka

We will use Docker to run Kafka

Get our Docker image of interest by running the following command:

`docker pull spotify/kafka`

Run Kafka docker image:

`bash docker run -p 2181:2181 -p 9092:9092 spotify/kafka`

Run bash session in a running docker container:

`docker exec -it [idOfRunningContainer] bash`

While in bash, setup some Kafka topics:

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic DataIn
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic DataOut
```

Hint: you can find the kafka sh commands in /opt folder of the running container.

### Run hydra
 
 ```bash
sbt assembly
java -jar target/scala-2.12/hydra.jar 
 ```

### Send some data
```bash
bin/kafka-console-producer.sh --broker-list localhost:9092 --topic DataIn
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic DataOut --from-beginning
```

 
# Join in!

We are happy to receive bug reports, fixes, documentation enhancements,
and other improvements.

Please report bugs via the
[github issue tracker](http://github.com/carldata/hydra/issues).



# Redistributing

Hydra source code is distributed under the Apache-2.0 license.

**Contributions**

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
licensed as above, without any additional terms or conditions.
