package app.dao.entity

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Row
//import  org.apache.spark.implicits._

import scala.collection.immutable.Map
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType



case class BusinessInfoEntity(address:String, 
              attributes_map:Map[String,String],
              business_id:String,
              categories:List[String],
              city:String,
              hours_map:Map[String,String],
              is_open:Boolean,
              latitude:Double,
              longitude:Double, 
              name:String,
              postal_code: String,
              review_count: Long,
              stars: Double,
              state:String
)


