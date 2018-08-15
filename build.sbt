name := "spark_project"

version := "0.1"

scalaVersion := "2.11.12"

val sparkVersion: String = "2.3.1"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)
