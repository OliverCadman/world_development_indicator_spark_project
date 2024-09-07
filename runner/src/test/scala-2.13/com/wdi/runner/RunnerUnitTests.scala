package com.wdi.runner

import io.circe.generic.codec.DerivedAsObjectCodec.deriveCodec
import org.apache.spark.sql.SparkSession
import org.scalatest.Inside._
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues



class RunnerUnitTests extends AnyWordSpec
  with Matchers with EitherValues {

  case class TestConfigClass(
                              foo: Int,
                              bar: String
                            )

  // Required just for mocking
  case class TestCaseClass()

  object TestRunner extends ScriptRunner[TestCaseClass] {
    def run(config: TestCaseClass, sparkSession: SparkSession): Unit = ???
  }

  object TestRunnerForDecode extends ScriptRunner[TestConfigClass] {
    override def run(config: TestConfigClass, sparkSession: SparkSession): Unit = ???
  }

  "ScriptRunner.load" should {
    "Return a typesafe.config Config instance containing the values 'foo' and 'bar'" in {

      val config = TestRunner.loadConfig("reference")

      val configObject = config.getObject("testObject")

      assert (configObject.containsKey("foo"))
      assert (configObject.containsKey("bar"))
    }
  }
  "ScriptRunner.decodeConfig" should {
    "Return an instance of TestConfigClass containing keys foo and bar, with expected values" in {

      val config = TestRunnerForDecode.loadConfig("reference")

      val testCaseClassInstance = TestConfigClass(
        foo = 42, bar = "test"
      )

      TestRunnerForDecode.decodeConfig(config, "testObject") should === (Right(testCaseClassInstance))
    }

    "Return an error if the requested config object does not exist" in {
      val config = TestRunner.loadConfig("test-config")
      val nonexistentPath = "nonExistent"

      val decodeAttempt = TestRunner.decodeConfig(config, nonexistentPath)

      decodeAttempt should matchPattern {
        case Left(_) =>
      }

      inside(decodeAttempt) {
        case Left(e) => {
          e.getMessage should ===("Path not found in config")
        }
      }
    }
  }
}
