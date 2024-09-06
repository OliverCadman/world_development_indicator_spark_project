ThisBuild / scalaVersion := "2.13.12"

lazy val commonSettings =  libraryDependencies ++= Seq(
  CoreDependencies.sparkCore,
  CoreDependencies.sparkSql,
  TestDependencies.scalaTest,
  TestDependencies.scalactic
)

lazy val preprocessor = (project in file ("preprocessor"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      CoreDependencies.sparkMLLib,
      CoreDependencies.sparkStreaming,
    )
  ).dependsOn(runner)

lazy val runner = (project in file ("runner"))
  .settings(commonSettings)
  .settings(
    libraryDependencies ++= Seq(
      ConfigDependencies.circeGeneric,
      ConfigDependencies.circeConfig,
      ConfigDependencies.typeSafeConfig,
    )
  )

lazy val root = (project in file (".")).aggregate(preprocessor, runner)
