package com.wdi.preprocess.projects

import com.wdi.preprocess.config.PreprocessConfig
import com.wdi.runner.ScriptRunner
import io.circe.generic.codec.DerivedAsObjectCodec.deriveCodec
import org.apache.spark.sql.SparkSession

object PreprocessProjectsCSV extends ScriptRunner[PreprocessConfig]{

  override def run(
                    config: PreprocessConfig,
                    sparkSession: SparkSession
                  ): Unit = {

    val projectsDataRaw = sparkSession.read.option("header", "true").csv(config.inputPath)

    projectsDataRaw.show(false)
  }
}
