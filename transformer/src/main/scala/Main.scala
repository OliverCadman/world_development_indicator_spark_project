import population_data.TransformPopulationData

object Main extends App {

  TransformPopulationData.executeScript("reference.conf", "transformer.inputOutput")
}
