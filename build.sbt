name := "spark-query-dsl"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.3"

libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "tests",
    "org.apache.spark" %% "spark-core" % sparkVersion % Test classifier "test-sources",
    "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
    "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "tests",
    "org.apache.spark" %% "spark-sql" % sparkVersion % Test classifier "test-sources",
    "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "tests",
    "org.apache.spark" %% "spark-catalyst" % sparkVersion % Test classifier "test-sources",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",
    "org.scalatest" %% "scalatest" % "3.0.8" % "test"
)

