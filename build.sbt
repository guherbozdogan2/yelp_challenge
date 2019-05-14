name := "Yelp Conversion task"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

enablePlugins(JavaAppPackaging)
enablePlugins(DockerPlugin)
resolvers ++= Seq(
  "apache-snapshots" at "http://repository.apache.org/snapshots/"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-hive" % sparkVersion,
  "mysql" % "mysql-connector-java" % "5.1.6"
)
resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0"

libraryDependencies += "com.lightbend.akka" %% "akka-stream-alpakka-cassandra" % "1.0.0"	

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "yelp_challenge",
  scalaVersion := "2.11.8",
  test in assembly := {}
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("com.example.Main"),
    // more settings here ...
 )



enablePlugins(JavaAppPackaging, AshScriptPlugin)
daemonUserUid in Docker := None
daemonUser in Docker := "daemon"
dockerBaseImage := "openjdk:8-jre-alpine"
dockerUpdateLatest := true
maintainer in Docker := "Guher Bozdogan <guher.bozdogan.wrk2@gmail.com>"
packageSummary in Docker := "yelp challenge sample db migration"
packageDescription := "Docker micro Service"


packageName in Docker := "repo"