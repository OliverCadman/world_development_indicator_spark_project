package com.wdi.runner

import com.typesafe.config.ConfigException
import io.circe.ParsingFailure
import io.circe.generic.codec.DerivedAsObjectCodec.deriveCodec
import org.scalactic.TypeCheckedTripleEquals
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

  val runner = new ScriptRunner[TestConfigClass]

  "ScriptRunner.load" should {
    "Return a typesafe.config Config instance containing the values 'foo' and 'bar'" in {

      val config = runner.loadConfig("tes-config")
      println(config)
      val configObject = config.getObject("testObject")

      assert (configObject.containsKey("foo"))
      assert (configObject.containsKey("bar"))
    }
  }
  "ScriptRunner.decodeConfig" should {
    "Return an instance of TestConfigClass containing keys foo and bar, with expected values" in {
      val config = runner.loadConfig("test-config")

      val testCaseClassInstance = TestConfigClass(
        foo = 42, bar = "test"
      )

      runner.decodeConfig(config, "testObject") should === (Right(testCaseClassInstance))
    }

    "Return an error if the requested config object does not exist" in {
      val config = runner.loadConfig("test-config")
      val nonexistentPath = "nonExistent"

      val decodeAttempt = runner.decodeConfig(config, nonexistentPath)

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
