import sbt._

object TestDependencies {

  val scalacticVersion = "3.2.19"
  val scalaTestVersion = "3.2.19"

  val scalactic = "org.scalactic" %% "scalactic" % scalacticVersion
  val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion % "test"
}
