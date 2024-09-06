package com.wdi.runner

import io.circe.generic.codec.DerivedAsObjectCodec.deriveCodec
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException
import org.scalactic.TypeCheckedTripleEquals
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.EitherValues


class RunnerIntegrationTests extends AnyWordSpec
  with Matchers with TypeCheckedTripleEquals with EitherValues {

  "ScriptRunner.executeScript" should {
    "Return an error message when a config path doesn't exist" in {
      case class TestCaseClass()
      val runner = new ScriptRunner[TestCaseClass]

      assert(runner.executeScript("nonexistent.conf", "nonexistent").isLeft)
    }
  }
}
