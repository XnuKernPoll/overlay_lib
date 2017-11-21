scalaVersion := "2.11.8" 

organization := "io.github.xnukernpoll"

name := "topologies"

version := "0.1"


val circeVersion = "0.8.0"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)



libraryDependencies ++= Seq (
  "com.twitter" % "finagle-mux_2.11" % "6.43.0",

  "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"
)
