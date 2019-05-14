addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.3.21")
//libraryDependencies += "com.spotify" % "docker-client" % "8.9.0"

//lazy val packager =  ProjectRef(file("../.."), "sbt-native-packager")
//dependsOn(packager)

// needs to be added for the docker spotify client
libraryDependencies += "com.spotify" % "docker-client" % "3.5.13"