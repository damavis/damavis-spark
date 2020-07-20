lazy val scala212 = "2.12.11"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

val sparkVersion = "3.0.0"
val sparkTestVersion = "2.4.5"

val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-avro" % sparkVersion % "provided",
  "com.typesafe" % "config" % "1.3.2"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.scalamock" %% "scalamock" % "4.1.0" % Test,
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkTestVersion}_0.14.0" % Test
)

val settings = Seq(
  organization := "com.damavis",
  version := "0.1.0-SNAPSHOT",
  isSnapshot := version.value.endsWith("SNAPSHOT"),
  scalaVersion := "2.12.11",
  libraryDependencies ++= dependencies ++ testDependencies,
  fork in Test := true,
  parallelExecution in Test := false,
  envVars in Test := Map(
    "MASTER" -> "local[*]"
  ),
  test in assembly := {}
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
  .settings(
    crossScalaVersions := supportedScalaVersions,
  )
