name := "kafkaConsumer"

version := "0.1"

scalaVersion := "2.12.2"

libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.1.0"
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.14.1"
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.14.1"
libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.21",
  "ch.qos.logback" % "logback-classic" % "1.2.3"
)
