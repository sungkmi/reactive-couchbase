name := """reactive-couchbase"""

version := "1.0-SNAPSHOT"

version := "0.1-SNAPSHOT"

organization := "com.manaolabs"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.1"

scalacOptions ++= Seq("-feature", "-deprecation")

crossScalaVersions ++= Seq("2.10.4", "2.11.2")

libraryDependencies ++= Seq(
  "com.couchbase.client" % "couchbase-client" % "1.4.+",
  "org.scalatestplus" %% "play" % "1.1.+" % "test"
)

publishTo := {
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some(Resolver.file("snapshot", Path.userHome / "repository" / "snapshots" asFile))
  else
    Some(Resolver.file("releases", Path.userHome / "repository" / "releases" asFile))
}
