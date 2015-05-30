package rivanov.spark.handson

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.SparkContext

class CsvDataFrames(countriesFile: String,
                    dataFile: String,
                    override val sc: SparkContext,
                    override val countryAlias: String = "c",
                    override val dataAlias: String = "d") extends CsvHelper with ColumnsDSL {

  val sqlContext = new SQLContext(sc)

  //DataFrames
  val countriesDF = csvDF(countriesFile, "countries").as(countryAlias)
  val dataDF = csvDF(dataFile, "data").as(dataAlias)

  //Columns names
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
  val asInt2005 = _2005.intCast
  val isValidNum2005 = asInt2005.isNotNull
  val notEmptyLongName = longName.notEmpty


  def dataFramesQuery(take: Int = 5) =
    countriesDF.join(dataDF, cCountryCode <~> dCountryCode)
      .filter(isValidNum2005)
      .select(asInt2005.as(kWh), shortName, region, longName, indicatorName)
      .filter(notEmptyLongName)
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


}

case class Result(kWh: Long, countryName: String, region: String, longCountryName: String)

object Result {
  def apply(row: Row): Result = Result(row.getLong(0), row.getString(1), row.getString(2), row.getString(3))
}
