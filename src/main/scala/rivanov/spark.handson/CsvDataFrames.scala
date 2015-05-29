package rivanov.spark.handson

import org.apache.spark.sql.{Column, Row, DataFrame, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.SparkContext

import scala.reflect.io.File

class CsvDataFrames(sc: SparkContext, countriesFile: String, dataFile: String, countryAlias: String = "c", dataAlias: String = "d") {

  val sqlContext = new SQLContext(sc)

  import CsvDataFrames._
  import sqlContext.implicits._

  //DataFrames

  val countriesDF = csvDF(countriesFile, "countries").as(countryAlias)
  val dataDF = csvDF(dataFile, "data").as(dataAlias)


  //Data fields

  val cCountryCode = s"$countryAlias.Country Code"
  val shortName = s"$countryAlias.Short Name"
  val region = s"$countryAlias.Region"
  val longName = s"$countryAlias.Long Name"
  val kWh = "kWh"

  val _2005 = s"$dataAlias.2005"
  val dCountryCode = s"$dataAlias.Country Code"
  val indicatorName = s"$dataAlias.Indicator Name"


  //Implicit helpers

  implicit val toColumn: String => Column = s => s.col

  implicit class ColumnConversions(column: String) {
    def col: Column = Symbol(column)

    def notEmpty: Column = column.col !== ""

    def intCast: Column = column.col.cast("bigint")
  }


  //Rules

  val regionRestriction = region.notEmpty && (region.col !== "World")
  val indicatorRestriction = indicatorName === "Electricity production (kWh)"
  val notEmpty2005 = _2005.notEmpty
  val notEmptyLongName = longName.notEmpty


  //Query functions

  def dataFramesQuery() =
    countriesDF.join(dataDF, cCountryCode.col === dCountryCode.col)
      .filter(notEmpty2005)
      .filter(notEmptyLongName)
      .select(_2005.intCast.as(kWh), shortName, region, longName, indicatorName)
      .where(indicatorRestriction)
      .where(regionRestriction)
      .groupBy(kWh, shortName, region, longName)
      .max(kWh)
      .orderBy(kWh.desc)
      .take(5)
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
