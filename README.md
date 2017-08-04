# ScriptEngine

[![Build status](https://travis-ci.org/carldata/hydra.svg?branch=master)](https://travis-ci.org/carldata/hydra)

Stream processing engine which executes custom script written in [FlowScript](http://github.com/carldata/flow-script).

Interface to this engine is based on Kafka
 
## Running the server
 
 ```bash
sbt assembly
java -jar target/scala-2.12/hydra-assembly-0.1.0.jar 
docker build -t hydra:0.1.0 .
docker run -p 8080:8080 hydra:0.1.0
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
