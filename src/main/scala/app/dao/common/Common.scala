package app.dao.common
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import scala.collection.immutable.Map
import org.apache.spark.sql.functions.{ col, udf }
object CommonUDF {

  val rowToMap: Row => Map[String, String] = (row: Row) => {
    if (row == null) {
      Map()
    } else {
      row.getValuesMap[String](row.schema.fieldNames.filter(field => !row.isNullAt(row.fieldIndex(field))).toSeq)
    }

  }
  val udfRowToMap = udf(rowToMap)

  val seconds_to_miliseconds: Long => Long = (column: Long) => {
    column * 1000
  }
  val udf_seconds_to_miliseconds = udf(seconds_to_miliseconds)

  val trim_array_entities: Seq[String] => Seq[String] = (entities: Seq[String]) => {

    entities.map(s => s.replaceAll("^\\s+", "")).map(s => s.replaceAll("\\s+$", ""))
  }
  val udf_trim_array_entities = udf(trim_array_entities)

}