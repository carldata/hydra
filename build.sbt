name := "hydra"

version := "0.1.0"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-streams" % "0.11.0.0",
  "com.outworkers" %% "phantom-dsl" % "2.12.1" exclude("org.slf4j","log4j-over-slf4j"),
  "io.spray" %% "spray-json" % "1.3.3" ,
  "io.github.carldata" %% "hydra-streams" % "0.3.0",
  "io.github.carldata" %% "flow-script" % "0.6.0",

  // Test dependencies
  "com.madewithtea" %% "mockedstreams" % "1.3.0" % "test",
  "org.scalatest" %% "scalatest" % "3.0.1" % "test"

)

assemblyJarName in assembly := "hydra.jar"