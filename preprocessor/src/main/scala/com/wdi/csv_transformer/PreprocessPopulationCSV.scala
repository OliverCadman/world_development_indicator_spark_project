package com.wdi.csv_transformer

import com.wdi.preprocess.CSVCleaner
import com.wdi.csv_transformer.TransformConfig
import com.wdi.runner.ScriptRunner
import io.circe.generic.decoding.DerivedDecoder.deriveDecoder
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object PreprocessPopulationCSV extends ScriptRunner[TransformConfig] {

  def run(
           config: TransformConfig,
           sparkSession: SparkSession
         ): Unit = {
    println(config)


    def getRow(x: Array[String], size: Int): Row = {
      val colArray: Array[String] = new Array[String](size)
      for (i <- 0 until size) {
        colArray(i) = x(i)
      }
      Row.fromSeq(colArray)
    }

    Logger.getLogger(getClass.getName).setLevel(Level.ERROR)

    val csvCleaner: CSVCleaner = new CSVCleaner(config.populationInput)

    // Remove first
    val initialCleanedRDD: RDD[String] = csvCleaner.run(sparkSession)

    val columnHeaders = initialCleanedRDD.zipWithIndex().filter(_._2 == 0).map(x => x._1.replaceAll("\"", "")).collect().mkString("").split(",")
    val fields = columnHeaders.map {
      field => StructField(field, StringType, nullable = true)
    }

    val schema = StructType(fields)

    // Turn lines of string into fields delimited by comma, but ignore commas that are included in a field.
    val parsedRDDCountryAndCodes = initialCleanedRDD.map {
        line =>
          line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
      }.map(attr => getRow(attr, fields.length))
      .zipWithIndex().filter((x) => x._2 != 0).map(_._1)


    val finalPopulationDF = sparkSession.createDataFrame(parsedRDDCountryAndCodes, schema)


    finalPopulationDF.write.mode("overwrite").parquet(config.populationOutput)
  }
}
