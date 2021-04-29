import sbt.librarymanagement.ivy.Credentials

object Publish {
  val USERNAME: String = sys.env.getOrElse("USERNAME", "")
  val PASSWORD: String = sys.env.getOrElse("PASSWORD", "")

  val credentials =
    Credentials(
      "Sonatype Nexus Repository Manager",
      "oss.sonatype.org",
      USERNAME,
      PASSWORD)

}
