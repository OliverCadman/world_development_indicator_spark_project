import sbt._

object ConfigDependencies {

  def circeBranch(branch: String): ModuleID = {
    val circeVersion = "0.13.0"
    val circeConfigVersion = "0.8.0"
    branch match {
      case i if i.contains("circe-config") => "io.circe" %% branch % circeConfigVersion
      case _ => "io.circe" %% branch % circeVersion
    }
  }

  val circeGeneric = circeBranch("circe-generic")
  val circeConfig = circeBranch("circe-config")

  val typeSafeConfigVersion = "1.4.3"
  val typeSafeConfig = "com.typesafe" %"config" % typeSafeConfigVersion
}
