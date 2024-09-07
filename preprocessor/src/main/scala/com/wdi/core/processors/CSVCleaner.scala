package com.wdi.core.processors

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

class CSVCleaner(path: String) extends CleanseCSV with CleanRDD {

  override def run(
                  session: SparkSession
                  ): RDD[String] = {

    val populationDirtyRowsRDD = session.sparkContext.textFile(path)
    val populationRowsDroppedRDD = dropRowsFromRDD(populationDirtyRowsRDD, 4)

    removeTrailingComma(populationRowsDroppedRDD)
  }
}
