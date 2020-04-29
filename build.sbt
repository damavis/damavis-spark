val sparkVersion = "2.4.5"

version := "0.1.0"

val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.typesafe" % "config" % "1.3.2"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.scalamock" %% "scalamock" % "4.1.0" % Test
)

val settings = Seq(
  organization := "com.damavis",
  version := "0.0.1",
  isSnapshot := version.value.endsWith("SNAPSHOT"),
  scalaVersion := "2.12.11",
  libraryDependencies ++= dependencies ++ testDependencies,
  fork in Test := true,
  envVars in Test := Map(
    "MASTER" -> "local[*]"
  )
)

lazy val root = (project in file("."))
  .settings(name := "damavis-spark")
  .settings(settings)
  .settings(publishArtifact := false)
  .aggregate(core)
  .dependsOn(core)

lazy val core = (project in file("damavis-spark-core"))
  .settings(settings)
  .settings(name := "damavis-spark-core")
