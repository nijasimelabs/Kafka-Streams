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
libraryDependencies += "org.slf4j" % "slf4j-log4j12" % "1.7.25"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.5.0"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.9"

val kafka_streams_scala_version = "0.2.1"

libraryDependencies ++= Seq("com.lightbend" %%
  "kafka-streams-scala" % kafka_streams_scala_version)
