package rivanov.spark.handson

import org.apache.spark.sql.{Column, SQLContext}

/**
 * Provides usability and abstraction on top of DataFrames Columns in order to work only with column names
 */
trait DataFramesDSL {

  val sqlContext: SQLContext

  val countryAlias: String
  val dataAlias: String

  import sqlContext.implicits._

  implicit class ColumnsDSL(column: String) {
    def col: Column = Symbol(column)

    def notEmpty: Column = ~!~("")

    def intCast: Column = column.col.cast("bigint")

    def <~>(other: Column): Column = column === other

    def ~~(value: String): Column = column === value

    def ~!~(value: String): Column = column !== value
  }

  implicit val toColumn: String => Column = s => s.col

  implicit class ColumnNamesInterpolator(val sc: StringContext) {
    def c(args: Any*): String = s"$countryAlias.${sc.parts.head}"

    def d(args: Any*): String = s"$dataAlias.${sc.parts.head}"
  }

}
