name := """reactive-couchbase"""

version := "0.5-SNAPSHOT"

organization := "com.manaolabs"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.6"

scalacOptions ++= Seq("-feature", "-deprecation")

crossScalaVersions ++= Seq("2.10.5", "2.11.6")

libraryDependencies ++= Seq(
  "com.couchbase.client" % "java-client" % "2.1.1",
  "com.google.inject" % "guice" % "4.0-beta5",
  "io.reactivex" %% "rxscala" % "0.24.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalatestplus" %% "play" % "1.2.0" % "test"
)

publishTo := {
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some(Resolver.file("snapshot", Path.userHome / "repository" / "snapshots" asFile))
  else
    Some(Resolver.file("releases", Path.userHome / "repository" / "releases" asFile))
}

scalariformSettings
