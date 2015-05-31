package rivanov.spark.handson

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.specs2.mutable.Specification

class JoinDataFramesSpec extends Specification {

  sequential
  stopOnFail

  // Specify correct location of unzipped files
  val countriesFile = "/Users/roman/Downloads/work/countries.csv"
  val dataFile = "/Users/roman/Downloads/work/data.csv"

  "Joining two CSV DataFrames " should {
    val sc: SparkContext = new SparkContext("local[*]", "JoinDF", new SparkConf())
    val app = new CsvDataFrames(countriesFile, dataFile, sc)
    import app.sqlContext.implicits._

    "filtering non integer rows when bigint cast is expected should work" in {
      val schema = StructType(Seq(StructField("2005", StringType, nullable = true)))
      val rdd = sc.parallelize(Seq("1234", "asdf", " ", "", "3456")).map(Row.apply(_))
      val df = app.sqlContext.createDataFrame(rdd, schema)
      df.registerTempTable("test2005")
      val rows: Array[Long] = df.as("d").select(app.asInt2005.as(app.kWh)).filter(Symbol(app.kWh).isNotNull).map(_.getLong(0)).collect()
      rows.length must_== 2
      rows(0) must_== 1234
      rows(1) must_== 3456
    }

    "Query with SQL and DataFrames API should produce same results" in {

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
