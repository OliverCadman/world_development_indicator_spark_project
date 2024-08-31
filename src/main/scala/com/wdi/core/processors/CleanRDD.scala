package com.wdi.core.processors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

trait CleanRDD {
  def dropRowsFromRDD(rdd: RDD[String], numRows: Int): RDD[String] = {
    val rddWithIndex = rdd.zipWithIndex().map(tup => (tup._2, tup._1)).sortByKey()

    rddWithIndex.filter(_._1 >= numRows).map(tup => tup._2)
  }

  def removeTrailingComma(rdd: RDD[String]): RDD[String] = {
    rdd.map {
      row => row.replaceFirst(",+$", "")
    }
  }

  def run(session: SparkSession): RDD[String]
}

abstract class CleanseCSV