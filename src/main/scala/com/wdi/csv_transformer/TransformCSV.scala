package com.wdi.csv_transformer

import org.apache.log4j._
import org.apache.spark.sql._

object TransformCSV extends App {

  val spark = SparkSession
    .builder()
    .appName("TransformCSV")
    .master("local[*]")
    .getOrCreate()

  Logger.getLogger(getClass.getName).setLevel(Level.ERROR)

  val populationDataDF = spark.read.csv()


}
