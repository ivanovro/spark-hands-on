package rivanov.spark.handson

import org.apache.spark.{SparkConf, SparkContext}
import org.specs2.mutable.Specification

class JoinDataFramesSpec extends Specification {

  sequential

  // Specify correct location of unzipped files
  val countriesFile = "/Users/roman/Downloads/work/countries.csv"
  val dataFile = "/Users/roman/Downloads/work/data.csv"

  "Joining two CSV DataFrames " should {

    "it should be possible to query with SQL" in {
      val sc: SparkContext = new SparkContext("local[*]", "JoinDF", new SparkConf())
      val app = new CsvDataFrames(sc, countriesFile, dataFile)

//      val results = app.runMajorSqlQuery()
      val results = app.runRddApiQuery()

      results.length must_== 5
      results.foldLeft((true, Long.MaxValue))((a, x) => (a._2 > x.getLong(0), x.getLong(0)))._1 must beTrue
      results.head(1).toString must_== "United States"

      println(f"${"kWh"}%15s ${"Country"}%25s ${"Region"}%25s ${"Long Name"}%30s")
      results.foreach(r => println(f"${r(0)}%15s ${r(1)}%25s ${r(2)}%25s ${r(3)}%30s"))

      sc.stop()
      success
    }

  }

}
