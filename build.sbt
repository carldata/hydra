name := "hydra"

version := "0.1.0"

scalaVersion := "2.12.3"

//libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.11.0.0"
libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % "0.11.0.0",
  "io.github.carldata" %% "hydra-streams" % "0.1.0",
  "com.madewithtea" %% "mockedstreams" % "1.3.0" % "test",
  "io.github.carldata" %% "flow-script" % "0.6.0",
  "com.outworkers"        %%  "phantom-dsl"               % "2.12.1",
  "io.spray" %%  "spray-json" % "1.3.3",


  // Test dependencies
  "org.scalatest" %% "scalatest" % "3.0.1" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"

)
assemblyJarName in assembly := "hydra.jar"