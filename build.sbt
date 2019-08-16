ThisBuild / version := "0.1"

ThisBuild / organization := "com.tubitv"
ThisBuild / scalaVersion := "2.11.12"

lazy val livytest = (project in file("."))
  .settings(
    name := "livytest",
    libraryDependencies += "org.apache.livy" % "livy-client-http" % "0.6.0-incubating",
    libraryDependencies += "org.apache.livy" %% "livy-scala-api" % "0.6.0-incubating",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.3"
  )