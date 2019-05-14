package app.dao.common
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import scala.collection.immutable.Map
import org.apache.spark.sql.functions.{ col, udf }
object CommonUDF {
  
  def successReturn():Int = 1
  def failureReturn():Int = 0

  val rowToMap: Row => Map[String, String] = (row: Row) => {
    if (row == null) {
      Map()
    } else {
      row.getValuesMap[String](row.schema.fieldNames.filter(field => !row.isNullAt(row.fieldIndex(field))).toSeq)
    }

  }
  val udfRowToMap = udf(rowToMap)

  val secondsToMiliseconds: Long => Long = (column: Long) => {
    column * 1000
  }
  val udfSecondsToMiliseconds = udf(secondsToMiliseconds)

  val trimArrayEntities: Seq[String] => Seq[String] = (entities: Seq[String]) => {

    entities.map(s => s.replaceAll("^\\s+", "")).map(s => s.replaceAll("\\s+$", ""))
  }
  val udfTrimArrayEntities = udf(trimArrayEntities)

}