name := "sparkwordcount"

version := "0.1"

scalaVersion := "2.11.12"
resolvers += "Hortonworks Repository" at "http://repo.hortonworks.com/content/repositories/releases/"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2" 
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.3.0" % "provided"
libraryDependencies += "it.nerdammer.bigdata" % "spark-hbase-connector_2.10" % "1.0.3"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.3.5"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.3.5"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.3.5"
libraryDependencies += "com.hortonworks.shc" % "shc-core" % "1.1.0.3.1.5.16-3"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.2"  % "provided"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) => MergeStrategy.discard
  case x => MergeStrategy.first
}