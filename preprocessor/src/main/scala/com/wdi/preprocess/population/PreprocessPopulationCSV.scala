package com.wdi.preprocess

import com.wdi.core.processors.CSVCleaner
import com.wdi.preprocess.config.PreprocessConfig
import com.wdi.runner.ScriptRunner
import io.circe.generic.decoding.DerivedDecoder.deriveDecoder
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object PreprocessPopulationCSV extends ScriptRunner[PreprocessConfig] {

  def run(
           config: PreprocessConfig,
           sparkSession: SparkSession
         ): Unit = {

    def getRow(x: Array[String], size: Int): Row = {
      val colArray: Array[String] = new Array[String](size)
      for (i <- 0 until size) {
        colArray(i) = x(i)
      }
      Row.fromSeq(colArray)
    }

    Logger.getLogger(getClass.getName).setLevel(Level.ERROR)

    val csvCleaner: CSVCleaner = new CSVCleaner(config.inputPath)

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


    val populationDF = sparkSession.createDataFrame(parsedRDDCountryAndCodes, schema)

    val finalPopulationDF = populationDF.columns.foldLeft(populationDF) {
      (tempDF, colName) => {
        tempDF.withColumn(colName, regexp_replace(col(colName).cast("string"), "\"", ""))
      }
    }

    finalPopulationDF.show(false)

    finalPopulationDF.write.mode("overwrite").parquet(config.outputPath)
  }
}
