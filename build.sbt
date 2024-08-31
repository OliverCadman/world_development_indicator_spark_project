ThisBuild / scalaVersion := "2.13.12"

val app = (project in file ("."))
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.sparkCore,
      Dependencies.sparkSql,
      Dependencies.sparkMLLib,
      Dependencies.sparkStreaming,
      Dependencies.twitter4jStream,
      Dependencies.twitter4jCore
    )
  )