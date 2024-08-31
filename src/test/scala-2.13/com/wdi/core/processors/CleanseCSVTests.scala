package com.wdi.core.processors

import com.utils.TestSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession


class CleanseCSVTests extends TestSuite {
  import CleanseCSVTests._

  "CleanserAPI.dropRowsFromRDD" should {
    "drop the first 4 rows of the raw dataset, and return an RDD with cleansed data" in {
      val testRDDBlankLines = sparkSession.sparkContext.textFile(inputPathBlankLines)
      val rddCleanerDropRows = new CleanRDD {
        override def run(session: SparkSession): RDD[String] = this.dropRowsFromRDD(testRDDBlankLines, 4)
      }

      val actual = rddCleanerDropRows.run(sparkSession).collect()
      val expected = expectedRDDRowsDropped.collect()

      actual should === (expected)
    }
  }
  "CleanserAPI.removeTrailingCommas" should {
    "remove any final trailing commas from rows in the raw CSV dataset" in {

      val testRDDTrailingCommas = sparkSession.sparkContext.textFile(inputPathTrailingCommas)
      val rddCleanerNoTrailingCommas = new CleanRDD {
        override def run(session: SparkSession): RDD[String] = this.removeTrailingComma(testRDDTrailingCommas)
      }

      val actual = rddCleanerNoTrailingCommas.run(sparkSession).collect()
      val expected = expectedRDDNoTrailingCommas.collect()

      actual should === (expected)
    }
  }
}

object CleanseCSVTests {
  private val sparkSession = SparkSession
    .builder
    .appName("CleanserAPITests")
    .master("local[*]")
    .getOrCreate()

  private val inputPathBlankLines = getClass.getResource("/population_data_raw_blank_lines.csv").getPath

  private val expectedRDDRowsDropped: RDD[String] = sparkSession.sparkContext.parallelize(
    List(
      "\"Country Name\",\"Country Code\",\"Indicator Name\",\"Indicator Code\",\"1960\",\"1961\",\"1962\",\"1963\",\"1964\",\"1965\",\"1966\",\"1967\",\"1968\",\"1969\",\"1970\",\"1971\",\"1972\",\"1973\",\"1974\",\"1975\",\"1976\",\"1977\",\"1978\",\"1979\",\"1980\",\"1981\",\"1982\",\"1983\",\"1984\",\"1985\",\"1986\",\"1987\",\"1988\",\"1989\",\"1990\",\"1991\",\"1992\",\"1993\",\"1994\",\"1995\",\"1996\",\"1997\",\"1998\",\"1999\",\"2000\",\"2001\",\"2002\",\"2003\",\"2004\",\"2005\",\"2006\",\"2007\",\"2008\",\"2009\",\"2010\",\"2011\",\"2012\",\"2013\",\"2014\",\"2015\",\"2016\",\"2017\",",
      "\"Aruba\",\"ABW\",\"Population, total\",\"SP.POP.TOTL\",\"54211\",\"55438\",\"56225\",\"56695\",\"57032\",\"57360\",\"57715\",\"58055\",\"58386\",\"58726\",\"59063\",\"59440\",\"59840\",\"60243\",\"60528\",\"60657\",\"60586\",\"60366\",\"60103\",\"59980\",\"60096\",\"60567\",\"61345\",\"62201\",\"62836\",\"63026\",\"62644\",\"61833\",\"61079\",\"61032\",\"62149\",\"64622\",\"68235\",\"72504\",\"76700\",\"80324\",\"83200\",\"85451\",\"87277\",\"89005\",\"90853\",\"92898\",\"94992\",\"97017\",\"98737\",\"100031\",\"100832\",\"101220\",\"101353\",\"101453\",\"101669\",\"102053\",\"102577\",\"103187\",\"103795\",\"104341\",\"104822\",\"105264\",",
      "\"Afghanistan\",\"AFG\",\"Population, total\",\"SP.POP.TOTL\",\"8996351\",\"9166764\",\"9345868\",\"9533954\",\"9731361\",\"9938414\",\"10152331\",\"10372630\",\"10604346\",\"10854428\",\"11126123\",\"11417825\",\"11721940\",\"12027822\",\"12321541\",\"12590286\",\"12840299\",\"13067538\",\"13237734\",\"13306695\",\"13248370\",\"13053954\",\"12749645\",\"12389269\",\"12047115\",\"11783050\",\"11601041\",\"11502761\",\"11540888\",\"11777609\",\"12249114\",\"12993657\",\"13981231\",\"15095099\",\"16172719\",\"17099541\",\"17822884\",\"18381605\",\"18863999\",\"19403676\",\"20093756\",\"20966463\",\"21979923\",\"23064851\",\"24118979\",\"25070798\",\"25893450\",\"26616792\",\"27294031\",\"28004331\",\"28803167\",\"29708599\",\"30696958\",\"31731688\",\"32758020\",\"33736494\",\"34656032\",\"35530081\",",
      "\"Angola\",\"AGO\",\"Population, total\",\"SP.POP.TOTL\",\"5643182\",\"5753024\",\"5866061\",\"5980417\",\"6093321\",\"6203299\",\"6309770\",\"6414995\",\"6523791\",\"6642632\",\"6776381\",\"6927269\",\"7094834\",\"7277960\",\"7474338\",\"7682479\",\"7900997\",\"8130988\",\"8376147\",\"8641521\",\"8929900\",\"9244507\",\"9582156\",\"9931562\",\"10277321\",\"10609042\",\"10921037\",\"11218268\",\"11513968\",\"11827237\",\"12171441\",\"12553446\",\"12968345\",\"13403734\",\"13841301\",\"14268994\",\"14682284\",\"15088981\",\"15504318\",\"15949766\",\"16440924\",\"16983266\",\"17572649\",\"18203369\",\"18865716\",\"19552542\",\"20262399\",\"20997687\",\"21759420\",\"22549547\",\"23369131\",\"24218565\",\"25096150\",\"25998340\",\"26920466\",\"27859305\",\"28813463\",\"29784193\",",
      "\"Albania\",\"ALB\",\"Population, total\",\"SP.POP.TOTL\",\"1608800\",\"1659800\",\"1711319\",\"1762621\",\"1814135\",\"1864791\",\"1914573\",\"1965598\",\"2022272\",\"2081695\",\"2135479\",\"2187853\",\"2243126\",\"2296752\",\"2350124\",\"2404831\",\"2458526\",\"2513546\",\"2566266\",\"2617832\",\"2671997\",\"2726056\",\"2784278\",\"2843960\",\"2904429\",\"2964762\",\"3022635\",\"3083605\",\"3142336\",\"3227943\",\"3286542\",\"3266790\",\"3247039\",\"3227287\",\"3207536\",\"3187784\",\"3168033\",\"3148281\",\"3128530\",\"3108778\",\"3089027\",\"3060173\",\"3051010\",\"3039616\",\"3026939\",\"3011487\",\"2992547\",\"2970017\",\"2947314\",\"2927519\",\"2913021\",\"2905195\",\"2900401\",\"2895092\",\"2889104\",\"2880703\",\"2876101\",\"2873457\",",
      "\"Andorra\",\"AND\",\"Population, total\",\"SP.POP.TOTL\",\"13411\",\"14375\",\"15370\",\"16412\",\"17469\",\"18549\",\"19647\",\"20758\",\"21890\",\"23058\",\"24276\",\"25559\",\"26892\",\"28232\",\"29520\",\"30705\",\"31777\",\"32771\",\"33737\",\"34818\",\"36067\",\"37500\",\"39114\",\"40867\",\"42706\",\"44600\",\"46517\",\"48455\",\"50434\",\"52448\",\"54509\",\"56671\",\"58888\",\"60971\",\"62677\",\"63850\",\"64360\",\"64327\",\"64142\",\"64370\",\"65390\",\"67341\",\"70049\",\"73182\",\"76244\",\"78867\",\"80991\",\"82683\",\"83861\",\"84462\",\"84449\",\"83751\",\"82431\",\"80788\",\"79223\",\"78014\",\"77281\",\"76965\",",
      "\"Arab World\",\"ARB\",\"Population, total\",\"SP.POP.TOTL\",\"92490932\",\"95044497\",\"97682294\",\"100411076\",\"103239902\",\"106174988\",\"109230593\",\"112406932\",\"115680165\",\"119016542\",\"122398374\",\"125807419\",\"129269375\",\"132863416\",\"136696761\",\"140843298\",\"145332378\",\"150133054\",\"155183724\",\"160392488\",\"165689490\",\"171051950\",\"176490084\",\"182005827\",\"187610756\",\"193310301\",\"199093767\",\"204942549\",\"210844771\",\"216787402\",\"224735446\",\"230829868\",\"235037179\",\"241286091\",\"247435930\",\"255029671\",\"260843462\",\"266575075\",\"272235146\",\"277962869\",\"283832016\",\"289850357\",\"296026575\",\"302434519\",\"309162029\",\"316264728\",\"323773264\",\"331653797\",\"339825483\",\"348145094\",\"356508908\",\"364895878\",\"373306993\",\"381702086\",\"390043028\",\"398304960\",\"406452690\",\"414491886\","
    )
  )

  private val inputPathTrailingCommas = getClass.getResource("/population_data_raw_trailing_commas.csv").getPath

  private val expectedRDDNoTrailingCommas: RDD[String] = sparkSession.sparkContext.parallelize(
    List(
      "\"Country Name\",\"Country Code\",\"Indicator Name\",\"Indicator Code\",\"1960\",\"1961\",\"1962\",\"1963\",\"1964\",\"1965\",\"1966\",\"1967\",\"1968\",\"1969\",\"1970\",\"1971\",\"1972\",\"1973\",\"1974\",\"1975\",\"1976\",\"1977\",\"1978\",\"1979\",\"1980\",\"1981\",\"1982\",\"1983\",\"1984\",\"1985\",\"1986\",\"1987\",\"1988\",\"1989\",\"1990\",\"1991\",\"1992\",\"1993\",\"1994\",\"1995\",\"1996\",\"1997\",\"1998\",\"1999\",\"2000\",\"2001\",\"2002\",\"2003\",\"2004\",\"2005\",\"2006\",\"2007\",\"2008\",\"2009\",\"2010\",\"2011\",\"2012\",\"2013\",\"2014\",\"2015\",\"2016\",\"2017\"",
      "\"Aruba\",\"ABW\",\"Population, total\",\"SP.POP.TOTL\",\"54211\",\"55438\",\"56225\",\"56695\",\"57032\",\"57360\",\"57715\",\"58055\",\"58386\",\"58726\",\"59063\",\"59440\",\"59840\",\"60243\",\"60528\",\"60657\",\"60586\",\"60366\",\"60103\",\"59980\",\"60096\",\"60567\",\"61345\",\"62201\",\"62836\",\"63026\",\"62644\",\"61833\",\"61079\",\"61032\",\"62149\",\"64622\",\"68235\",\"72504\",\"76700\",\"80324\",\"83200\",\"85451\",\"87277\",\"89005\",\"90853\",\"92898\",\"94992\",\"97017\",\"98737\",\"100031\",\"100832\",\"101220\",\"101353\",\"101453\",\"101669\",\"102053\",\"102577\",\"103187\",\"103795\",\"104341\",\"104822\",\"105264\"",
      "\"Afghanistan\",\"AFG\",\"Population, total\",\"SP.POP.TOTL\",\"8996351\",\"9166764\",\"9345868\",\"9533954\",\"9731361\",\"9938414\",\"10152331\",\"10372630\",\"10604346\",\"10854428\",\"11126123\",\"11417825\",\"11721940\",\"12027822\",\"12321541\",\"12590286\",\"12840299\",\"13067538\",\"13237734\",\"13306695\",\"13248370\",\"13053954\",\"12749645\",\"12389269\",\"12047115\",\"11783050\",\"11601041\",\"11502761\",\"11540888\",\"11777609\",\"12249114\",\"12993657\",\"13981231\",\"15095099\",\"16172719\",\"17099541\",\"17822884\",\"18381605\",\"18863999\",\"19403676\",\"20093756\",\"20966463\",\"21979923\",\"23064851\",\"24118979\",\"25070798\",\"25893450\",\"26616792\",\"27294031\",\"28004331\",\"28803167\",\"29708599\",\"30696958\",\"31731688\",\"32758020\",\"33736494\",\"34656032\",\"35530081\"",
      "\"Angola\",\"AGO\",\"Population, total\",\"SP.POP.TOTL\",\"5643182\",\"5753024\",\"5866061\",\"5980417\",\"6093321\",\"6203299\",\"6309770\",\"6414995\",\"6523791\",\"6642632\",\"6776381\",\"6927269\",\"7094834\",\"7277960\",\"7474338\",\"7682479\",\"7900997\",\"8130988\",\"8376147\",\"8641521\",\"8929900\",\"9244507\",\"9582156\",\"9931562\",\"10277321\",\"10609042\",\"10921037\",\"11218268\",\"11513968\",\"11827237\",\"12171441\",\"12553446\",\"12968345\",\"13403734\",\"13841301\",\"14268994\",\"14682284\",\"15088981\",\"15504318\",\"15949766\",\"16440924\",\"16983266\",\"17572649\",\"18203369\",\"18865716\",\"19552542\",\"20262399\",\"20997687\",\"21759420\",\"22549547\",\"23369131\",\"24218565\",\"25096150\",\"25998340\",\"26920466\",\"27859305\",\"28813463\",\"29784193\"",
      "\"Albania\",\"ALB\",\"Population, total\",\"SP.POP.TOTL\",\"1608800\",\"1659800\",\"1711319\",\"1762621\",\"1814135\",\"1864791\",\"1914573\",\"1965598\",\"2022272\",\"2081695\",\"2135479\",\"2187853\",\"2243126\",\"2296752\",\"2350124\",\"2404831\",\"2458526\",\"2513546\",\"2566266\",\"2617832\",\"2671997\",\"2726056\",\"2784278\",\"2843960\",\"2904429\",\"2964762\",\"3022635\",\"3083605\",\"3142336\",\"3227943\",\"3286542\",\"3266790\",\"3247039\",\"3227287\",\"3207536\",\"3187784\",\"3168033\",\"3148281\",\"3128530\",\"3108778\",\"3089027\",\"3060173\",\"3051010\",\"3039616\",\"3026939\",\"3011487\",\"2992547\",\"2970017\",\"2947314\",\"2927519\",\"2913021\",\"2905195\",\"2900401\",\"2895092\",\"2889104\",\"2880703\",\"2876101\",\"2873457\"",
      "\"Andorra\",\"AND\",\"Population, total\",\"SP.POP.TOTL\",\"13411\",\"14375\",\"15370\",\"16412\",\"17469\",\"18549\",\"19647\",\"20758\",\"21890\",\"23058\",\"24276\",\"25559\",\"26892\",\"28232\",\"29520\",\"30705\",\"31777\",\"32771\",\"33737\",\"34818\",\"36067\",\"37500\",\"39114\",\"40867\",\"42706\",\"44600\",\"46517\",\"48455\",\"50434\",\"52448\",\"54509\",\"56671\",\"58888\",\"60971\",\"62677\",\"63850\",\"64360\",\"64327\",\"64142\",\"64370\",\"65390\",\"67341\",\"70049\",\"73182\",\"76244\",\"78867\",\"80991\",\"82683\",\"83861\",\"84462\",\"84449\",\"83751\",\"82431\",\"80788\",\"79223\",\"78014\",\"77281\",\"76965\"",
      "\"Arab World\",\"ARB\",\"Population, total\",\"SP.POP.TOTL\",\"92490932\",\"95044497\",\"97682294\",\"100411076\",\"103239902\",\"106174988\",\"109230593\",\"112406932\",\"115680165\",\"119016542\",\"122398374\",\"125807419\",\"129269375\",\"132863416\",\"136696761\",\"140843298\",\"145332378\",\"150133054\",\"155183724\",\"160392488\",\"165689490\",\"171051950\",\"176490084\",\"182005827\",\"187610756\",\"193310301\",\"199093767\",\"204942549\",\"210844771\",\"216787402\",\"224735446\",\"230829868\",\"235037179\",\"241286091\",\"247435930\",\"255029671\",\"260843462\",\"266575075\",\"272235146\",\"277962869\",\"283832016\",\"289850357\",\"296026575\",\"302434519\",\"309162029\",\"316264728\",\"323773264\",\"331653797\",\"339825483\",\"348145094\",\"356508908\",\"364895878\",\"373306993\",\"381702086\",\"390043028\",\"398304960\",\"406452690\",\"414491886\""
    )
  )
}