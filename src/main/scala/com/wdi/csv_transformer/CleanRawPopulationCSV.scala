package com.wdi.csv_transformer

import com.wdi.preprocess.CSVCleaner
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}


object CleanRawPopulationCSV extends App {

  def getRow(x: Array[String], size: Int): Row = {
    val colArray: Array[String] = new Array[String](size)
    for (i <- 0 until size) {
      colArray(i) = x(i)
    }
    Row.fromSeq(colArray)
  }

  val spark = SparkSession
    .builder()
    .appName("TransformPopulationCSV")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger(getClass.getName).setLevel(Level.ERROR)

  val csvPath = "src/main/resources/population_data.csv"
  val csvCleaner: CSVCleaner = new CSVCleaner(csvPath)

  // Remove first
  val initialCleanedRDD: RDD[String] = csvCleaner.run(spark)

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


  val populationYearsRemovedDS = spark.createDataFrame(parsedRDDCountryAndCodes, schema)

  // All fields are wrapped in quotation marks. Remove them
  val quotesRemoved = populationYearsRemovedDS.columns.foldLeft(populationYearsRemovedDS) {
    (tempDF, colName) => tempDF.withColumn(colName, regexp_replace(col(colName), "\"+", ""))
  }

  val dfMelt = quotesRemoved.melt(
    ids = Array(col("Country Name"), col("Country Code"), col("Indicator Name"), col("Indicator Code")),
    variableColumnName = "Year",
    valueColumnName = "Population"
  )

  dfMelt.show()




}
