package com.wdi.csv_transformer.models

case class PopulationData(
                         countryName: Option[String],
                         countryCode: Option[String],
                         indicatorName: Option[String],
                         indicatorCode: Option[String],
                         populationPerYear: Seq[YearlyPopulation]
                         )
