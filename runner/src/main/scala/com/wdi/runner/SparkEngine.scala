package com.wdi.runner

import org.apache.spark.sql.SparkSession

class SparkEngine {
  def init(): SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .getOrCreate()
}
