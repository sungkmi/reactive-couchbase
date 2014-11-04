name := """reactive-couchbase"""

version := "0.2-SNAPSHOT"

organization := "com.manaolabs"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.4"

scalacOptions ++= Seq("-feature", "-deprecation")

crossScalaVersions ++= Seq("2.10.4", "2.11.4")

libraryDependencies ++= Seq(
  "com.couchbase.client" % "java-client" % "2.0.0",
  "com.google.inject" % "guice" % "4.0-beta5",
  "io.reactivex" % "rxjava" % "1.0.0-rc.9",
  "io.reactivex" %% "rxscala" % "0.22.0",
  "net.codingwell" %% "scala-guice" % "4.0.0-beta4",
  "org.scalatest" %% "scalatest" % "2.2.2" % "test",
  "org.scalatestplus" %% "play" % "1.2.0" % "test"
)

publishTo := {
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some(Resolver.file("snapshot", Path.userHome / "repository" / "snapshots" asFile))
  else
    Some(Resolver.file("releases", Path.userHome / "repository" / "releases" asFile))
}

scalariformSettings
