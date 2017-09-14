scalaVersion := "2.11.8" 

organization := "io.github.xnukernpoll"

name := "topologies"

version := "0.1"


libraryDependencies ++= Seq (
  
  "org.scodec" % "scodec-core_2.11" % "1.10.3",

  "com.twitter" % "finagle-mux_2.11" % "6.43.0",

  "org.scalatest" % "scalatest_2.11" % "3.0.4" % "test"

 

)
