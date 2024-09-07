package population_data

import com.wdi.runner.ScriptRunner
import io.circe.generic.codec.DerivedAsObjectCodec.deriveCodec
import org.apache.spark.sql.SparkSession

object TransformPopulationData extends ScriptRunner[TransformConfig]{

  def run(config: TransformConfig, sparkSession: SparkSession): Unit = {

    val populationData = sparkSession
      .read
      .option("header", "true")
      .parquet(config.populationDataInput)

    populationData.printSchema

  }
}
