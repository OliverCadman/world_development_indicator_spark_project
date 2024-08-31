package com.wdi.preprocess

import com.wdi.core.processors.{CleanRDD, CleanseCSV}
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StructField}

class CSVCleaner(path: String) extends CleanseCSV with CleanRDD {

  override def run(
                  session: SparkSession
                  ): RDD[String] = {

    val populationDirtyRowsRDD = session.sparkContext.textFile(path)
    val populationRowsDroppedRDD = dropRowsFromRDD(populationDirtyRowsRDD, 4)

    removeTrailingComma(populationRowsDroppedRDD)
  }
}
