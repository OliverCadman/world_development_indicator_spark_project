package com.wdi.runner

import org.apache.spark.sql.SparkSession
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class SparkEngineTests extends AnyWordSpec with Matchers {

  "SparkEngine.init" should {
    "Return a SparkSession object" in {
      val engine = new SparkEngine

      engine.init().getClass should === (
        SparkSession.getClass
      )
    }
  }
}
