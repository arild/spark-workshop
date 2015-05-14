name := "Spark workshop"

scalaVersion := "2.11.2"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.3.0"

libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.3.0"

libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.3.0" exclude("org.eclipse.jetty", "jetty-server")

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "1.3.0"

libraryDependencies += "org.scalatest" % "scalatest_2.11" % "2.2.3" % "test"

libraryDependencies += "org.slf4j" % "slf4j-api" % "1.7.10"

libraryDependencies += "joda-time" % "joda-time" % "2.7"

libraryDependencies += "org.joda" % "joda-convert" % "1.7"

//parallelExecution in Test := false

fork in Test := true