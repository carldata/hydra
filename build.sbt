name := "hydra"

version := "0.2.0"

scalaVersion := "2.12.3"

libraryDependencies ++= Seq(
  "org.scala-lang" % "scala-reflect" % "2.12.3",
  "org.apache.kafka" % "kafka-streams" % "0.11.0.0",
  "com.outworkers" %% "phantom-dsl" % "2.12.1" exclude("org.slf4j","log4j-over-slf4j"),
  "io.spray" %% "spray-json" % "1.3.3" ,
  "io.github.carldata" %% "hydra-streams" % "0.4.4",
  "io.github.carldata" %% "flow-script" % "0.7.8",
  "com.outworkers" %%  "phantom-dsl" % "2.12.1",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "de.siegmar" % "logback-gelf" % "1.0.4",
  "ch.qos.logback" % "logback-classic" % "1.2.3" % Runtime,
  "com.datadoghq" % "java-dogstatsd-client" % "2.3",

  // Test dependencies
  "com.madewithtea" %% "mockedstreams" % "1.3.0" % Test,
  "org.scalatest" %% "scalatest" % "3.0.1" % Test

)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyJarName in assembly := "hydra.jar"