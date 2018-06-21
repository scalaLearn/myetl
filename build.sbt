name := "myetl"

version := "0.1"

scalaVersion := "2.11.12"

assemblyJarName in assembly := "myetl.jar"

test in assembly := {}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

assemblyExcludedJars in assembly := {
  val cp = (fullClasspath in assembly).value
  cp
}



libraryDependencies ++= {
  val spark_version = "2.2.1"
  Seq(
    "com.fasterxml.jackson.core" % "jackson-core" % "2.9.1",
    "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.1",
    "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.1",
    "org.apache.spark" %% "spark-sql" % spark_version,
    "org.apache.spark" %% "spark-hive" % spark_version,
    "org.apache.spark" %%"spark-streaming" % spark_version,
    "org.apache.spark" %% "spark-mllib" % spark_version,
    "org.apache.spark" %% "spark-streaming-flume" % spark_version,
    "org.apache.spark" %% "spark-graphx" % spark_version,
    "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3",
    "org.apache.kafka" %% "kafka" % "1.0.0",
    "org.apache.sqoop" % "sqoop-client" % "1.99.7",
    "mysql" % "mysql-connector-java" % "5.1.45",
    "com.iheart" %% "ficus" % "1.4.3",
    "org.elasticsearch" %% "elasticsearch-spark-20" % "6.1.2",
    "org.apache.storm" % "storm" % "1.1.1",
    "org.scalatest" %% "scalatest" % "2.2.6" % "test",
    "commons-codec" % "commons-codec" % "1.11"
  )
}
