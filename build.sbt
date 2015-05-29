name := """reactive-couchbase"""

version := "0.6-SNAPSHOT"

organization := "com.manaolabs"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.6"

scalacOptions ++= Seq("-feature", "-deprecation")

crossScalaVersions ++= Seq("2.10.5", "2.11.6")

libraryDependencies ++= Seq(
  "com.couchbase.client" % "java-client" % "2.1.+",
  "io.reactivex" %% "rxscala" % "0.24.+",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalatestplus" %% "play" % "1.4.0-M+" % "test"
)

publishTo := {
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some(Resolver.file("snapshot", Path.userHome / "repository" / "snapshots" asFile))
  else
    Some(Resolver.file("releases", Path.userHome / "repository" / "releases" asFile))
}

scalariformSettings
