name := "hydra"

version := "0.1.0"

scalaVersion := "2.12.3"

libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.11.0.0"

assemblyJarName in assembly := "hydra.jar"