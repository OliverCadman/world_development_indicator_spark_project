package com.wdi.runner

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql._
import io.circe.config.syntax._

abstract class ScriptRunner[T : io.circe.Decoder] {

  def decodeConfig(config: Config, property: String): Either[io.circe.Error, T] = config.as[T](property)

  def loadConfig(referencePath: String): Config = {
    val referenceConf = ConfigFactory.parseResources(referencePath)
    val applicationConf = ConfigFactory.parseResources("application.conf")

    ConfigFactory.load().resolveWith(referenceConf.withFallback(applicationConf))
  }

  def executeScript(configPath: String, configProperty: String): Either[io.circe.Error, Unit] = {

    val config = loadConfig(configPath)

    println(config)

    decodeConfig(config, configProperty).map {
      config => {
        val sparkEngine = new SparkEngine
        run(config, sparkEngine.init())
      }
    }
  }

  // Implemented upon extension of ScriptRunner
  def run(
           config: T,
           sparkSession: SparkSession
         ): Unit
}


