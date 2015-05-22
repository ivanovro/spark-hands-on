package rivanov.spark.handson

import org.apache.spark.sql.{Row, DataFrame, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.SparkContext

class CsvDataFrames(sc: SparkContext, countriesFile: String, dataFile: String) {

  val sqlContext = new SQLContext(sc)

  import CsvDataFrames._
  import sqlContext.implicits._

  val countriesDF = csvDF(countriesFile, "countries")

  val dataDF = csvDF(dataFile, "data")


  def runMajorQuery() = sqlContext.sql( """
                                              |SELECT MAX(CAST(d.`2005` AS BIGINT)) AS `kWh`, c.`Short Name`, c.`Region`, c.`Long Name`
                                              |    FROM data d JOIN countries c ON (d.`Country Code` = c.`Country Code`)
                                              |      WHERE c.`Region` <> '' AND c.`Region` <> 'World'
                                              |      AND c.`Long Name` <> ''
                                              |      AND d.`2005` <> ''
                                              |      AND d.`Indicator Name` = 'Electricity production (kWh)'
                                              |      GROUP BY c.`Short Name`, c.`Region`, c.`Long Name`
                                            """.stripMargin).orderBy('kWh.desc).take(5)


  private def csvDF(filePath: String, tableName: String, delimiter: String = "\\|"): DataFrame = {

    val fileRdd = sc.textFile(filePath)

    val header = fileRdd.first().split(delimiter)
    val schema = StructType(header.map(field => StructField(field, StringType, nullable = true)))

    val csvRdd = fileRdd.map(csv => correctResult(csv.split(delimiter), header.length)).filter(skipHeader(header)).map(toRow)

    val df = sqlContext.createDataFrame(csvRdd, schema)
    df.registerTempTable(tableName)

    df
  }

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
