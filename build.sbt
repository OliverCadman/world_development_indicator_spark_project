ThisBuild / scalaVersion := "2.13.12"

val app = (project in file ("."))
  .settings(
    libraryDependencies ++= Seq(
      // Spark
      Dependencies.sparkCore,
      Dependencies.sparkSql,
      Dependencies.sparkMLLib,
      Dependencies.sparkStreaming,
      // Twitter4J
      Dependencies.twitter4jStream,
      Dependencies.twitter4jCore,
      // ScalaTest/Scalactic
      Dependencies.scalaTest,
      Dependencies.scalactic
    )
  )