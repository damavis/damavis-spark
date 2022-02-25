lazy val scala212 = "2.12.11"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

val sparkVersion = "3.0.0"
val sparkTestVersion = "2.4.5"

val dependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-hive" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-avro" % sparkVersion % Provided,
  "io.delta" %% "delta-core" % "0.7.0" % Provided,
  "com.typesafe" % "config" % "1.3.2"
)

val testDependencies = Seq(
  "org.scalatest" %% "scalatest" % "3.0.5" % Test,
  "org.scalamock" %% "scalamock" % "4.1.0" % Test,
  "com.holdenkarau" %% "spark-testing-base" % s"${sparkTestVersion}_0.14.0" % Test
)

import xerial.sbt.Sonatype._
val settings = Seq(
  organization := "com.damavis",
  version := "0.3.10",
  isSnapshot := version.value.endsWith("SNAPSHOT"),
  scalaVersion := "2.12.11",
  libraryDependencies ++= dependencies ++ testDependencies,
  fork in Test := true,
  parallelExecution in Test := false,
  envVars in Test := Map(
    "MASTER" -> "local[*]"
  ),
  test in assembly := {},
  // Sonatype
  sonatypeProfileName := "com.damavis",
  sonatypeProjectHosting := Some(
    GitHubHosting("damavis", "damavis-spark", "info@damavis.com")),
  publishMavenStyle := true,
  licenses := Seq(
    "APL2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  developers := List(
    Developer(id = "piffall",
              name = "Cristòfol Torrens",
              email = "piffall@gmail.com",
              url = url("http://piffall.com")),
    Developer(id = "priera",
              name = "Pedro Riera",
              email = "pedro.riera at damavis dot com",
              url = url("http://github.com/priera")),
  ),
  publishTo := sonatypePublishToBundle.value,
  credentials += Publish.credentials
)

lazy val root = (project in file("."))
  .settings(name := "damavis-spark")
  .settings(settings)
  .settings(publishArtifact := false)
  .aggregate(core, azure, snowflake)

lazy val core = (project in file("damavis-spark-core"))
  .settings(settings)
  .settings(name := "damavis-spark-core")
  .settings(
    crossScalaVersions := supportedScalaVersions,
  )

lazy val azure = (project in file("damavis-spark-azure"))
  .settings(settings)
  .settings(name := "damavis-spark-azure")
  .settings(
    crossScalaVersions := supportedScalaVersions,
  )
  .dependsOn(core)

lazy val snowflake = (project in file("damavis-spark-snowflake"))
  .settings(settings)
  .settings(name := "damavis-spark-snowflake")
  .settings(
    crossScalaVersions := supportedScalaVersions,
    libraryDependencies ++= Seq(
      "net.snowflake" %% "spark-snowflake" % "2.8.2-spark_3.0"
    )
  )
  .dependsOn(core)