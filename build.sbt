name := """reactive-couchbase"""

version := "0.6-SNAPSHOT"

organization := "com.manaolabs"

scalaVersion := "2.11.7"

scalacOptions ++= Seq("-feature", "-deprecation")

//lazy val root = (project in file(".")).enablePlugins(PlayScala)

libraryDependencies ++= Seq(
  "com.couchbase.client" % "java-client" % "2.1.4",
  "io.reactivex" % "rxjava" % "1.0.14",
  "io.reactivex" %% "rxscala" % "0.25.0",
  "net.codingwell" %% "scala-guice" % "4.0.0",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.scalatestplus" %% "play" % "1.4.0-M3" % "test"
)

publishTo := {
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some(Resolver.file("snapshot", Path.userHome / "repository" / "snapshots" asFile))
  else
    Some(Resolver.file("releases", Path.userHome / "repository" / "releases" asFile))
}

scalariformSettings
