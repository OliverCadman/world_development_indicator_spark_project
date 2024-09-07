package com

import com.wdi.preprocess.projects.PreprocessProjectsCSV

object Main extends App {
  PreprocessProjectsCSV.executeScript("reference.conf", "preprocess.projects")
}
