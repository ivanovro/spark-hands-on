package rivanov.spark.handson

import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.SparkContext

import scala.reflect.io.File

class CsvDataFrames(sc: SparkContext, countriesFile: String, dataFile: String, override val countryAlias: String = "c", override val dataAlias: String = "d") extends DataFramesDSL {

  val sqlContext = new SQLContext(sc)

  import CsvDataFrames._

  //DataFrames
  val countriesDF = csvDF(countriesFile, "countries").as(countryAlias)
  val dataDF = csvDF(dataFile, "data").as(dataAlias)

  //Data fields
  val cCountryCode = c"Country Code"
  val shortName = c"Short Name"
  val region = c"Region"
  val longName = c"Long Name"
  val _2005 = d"2005"
  val dCountryCode = d"Country Code"
  val indicatorName = d"Indicator Name"
  val kWh = "kWh"

  //Rules
  val regionRestriction = region.notEmpty && (region ~!~ "World")
  val indicatorRestriction = indicatorName ~~ "Electricity production (kWh)"
  val notEmpty2005 = _2005.notEmpty
  val notEmptyLongName = longName.notEmpty


  def dataFramesQuery(take: Int = 5) =
    countriesDF.join(dataDF, cCountryCode <~> dCountryCode)
      .filter(notEmpty2005)
      .filter(notEmptyLongName)
      .select(_2005.intCast.as(kWh), shortName, region, longName, indicatorName)
      .where(indicatorRestriction)
      .where(regionRestriction)
      .groupBy(kWh, shortName, region, longName)
      .max(kWh)
      .orderBy(kWh.desc)
      .take(take)
      .map(Result.apply)


  def sqlQuery(take: Int = 5) = sqlContext.sql( """
                                                  |SELECT MAX(CAST(d.`2005` AS BIGINT)) AS `kWh`, c.`Short Name`, c.`Region`, c.`Long Name`
                                                  |    FROM data d JOIN countries c ON (d.`Country Code` = c.`Country Code`)
                                                  |      WHERE c.`Region` <> '' AND c.`Region` <> 'World'
                                                  |      AND c.`Long Name` <> ''
                                                  |      AND d.`2005` <> ''
                                                  |      AND d.`Indicator Name` = 'Electricity production (kWh)'
                                                  |      GROUP BY c.`Short Name`, c.`Region`, c.`Long Name`
                                                  |      ORDER BY kWh DESC
                                                """.stripMargin).take(take).map(Result.apply)


  // Initialization utils

  private def csvDF(filePath: String, tableName: String, delimiter: String = "\\|"): DataFrame = {

    assert(File(filePath).exists, s"file '$filePath' is not available")

    val fileRdd = sc.textFile(filePath)

    val header = fileRdd.first().split(delimiter)
    val schema = StructType(header.map(field => StructField(field, StringType, nullable = true)))

    val csvRdd = fileRdd.map(csv => correctResult(csv.split(delimiter), header.length)).filter(skipHeader(header)).map(toRow)

    val df = sqlContext.createDataFrame(csvRdd, schema)
    df.registerTempTable(tableName)

    df
  }

}

case class Result(kWh: Long, countryName: String, region: String, longCountryName: String)

object Result {
  def apply(row: Row): Result = Result(row.getLong(0), row.getString(1), row.getString(2), row.getString(3))
}

object CsvDataFrames extends Serializable {

  val skipHeader: Array[String] => Array[String] => Boolean = header => csv => csv(0) != header(0)

  val correctResult: (Array[String], Int) => Array[String] = (csv, headerSize) => {
    val result = Array.fill(headerSize)("")
    csv.copyToArray(result)
    result
  }

  val toRow: Array[String] => Row = csv => Row.fromSeq(csv)
}
