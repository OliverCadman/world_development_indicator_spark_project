package com

import com.wdi.csv_transformer.PreprocessPopulationCSV

object Main extends App {
  PreprocessPopulationCSV.executeScript("reference.conf", "preprocess.inputOutput")
}
