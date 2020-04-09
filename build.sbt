name := "project-fdp"

version := "0.1"

scalaVersion := "2.11.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "org.apache.kafka" %% "kafka" % "2.4.1",
  "au.com.bytecode" % "opencsv" % "2.4"
)