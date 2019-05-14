package app.dao.entity

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Row
//import  org.apache.spark.implicits._

import scala.collection.immutable.Map
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import com.datastax.driver.core.LocalDate

case class TipEntity(
     tip_text: String,
	   tip_timestamp: Option[Long], 
	   compliment_count: Long, 
	   business_id: String,
	   user_id: String
	   )
    
    
    
