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

      val sqlResults = app.sqlQuery()
      val dfResults = app.dataFramesQuery()

      sqlResults mustEqual dfResults

      dfResults.length must_== 5
      dfResults.foldLeft((true, Long.MaxValue))((a, x) => (a._2 > x.kWh, x.kWh))._1 must beTrue
      dfResults.head.countryName.toString must_== "United States"

      println(f"${"kWh"}%15s ${"Country"}%25s ${"Region"}%25s ${"Long Name"}%30s")
      dfResults.foreach(r => println(f"${r.kWh}%15s ${r.countryName}%25s ${r.region}%25s ${r.longCountryName}%30s"))

      sc.stop()
      success
    }

  }

}
