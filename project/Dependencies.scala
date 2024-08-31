import sbt._


object Dependencies {

  def sparkBranch(branch: String): ModuleID = {
    val sparkVersion = "3.5.1"
    s"org.apache.spark" %% branch % sparkVersion
  }

  def twitter4jBranch(branch: String): ModuleID = {
    val twitterVersion = "4.0.7"
    s"org.twitter4j" % branch % twitterVersion
  }

  val sparkCore = sparkBranch("spark-core")
  val sparkSql = sparkBranch("spark-sql")
  val sparkMLLib = sparkBranch("spark-mllib")
  val sparkStreaming = sparkBranch("spark-streaming")

  val twitter4jCore = twitter4jBranch("twitter4j-core")
  val twitter4jStream = twitter4jBranch("twitter4j-stream")

}
