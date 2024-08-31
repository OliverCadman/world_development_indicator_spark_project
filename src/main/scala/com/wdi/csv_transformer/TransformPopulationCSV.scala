package com.wdi.csv_transformer

import com.wdi.preprocess.CSVCleaner
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

object TransformPopulationCSV extends App {

  val spark = SparkSession
    .builder()
    .appName("TransformPopulationCSV")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger(getClass.getName).setLevel(Level.ERROR)

  val csvPath = "src/main/resources/population_data.csv"
  val csvCleaner: CSVCleaner = new CSVCleaner(csvPath)

  val initialCleanedCSV: RDD[String] = csvCleaner.run(spark)
  initialCleanedCSV.foreach(x => {
    println(x)
    println("***")
  })

}
