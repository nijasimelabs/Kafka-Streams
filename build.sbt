name := "kafka-streams-scala"

version := "0.0.1-SNAPSHOT"

scalaVersion := "2.12.6"

scalacOptions := Seq("-Xexperimental", "-unchecked", "-deprecation")

enablePlugins(JavaAppPackaging)

// Kafka
libraryDependencies += "org.apache.kafka" % "kafka-streams" % "0.11.0.0"
// JSON
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.8"
// logging
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"

val kafka_streams_scala_version = "0.2.1"

libraryDependencies ++= Seq("com.lightbend" %%
  "kafka-streams-scala" % kafka_streams_scala_version)
