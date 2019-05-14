package app.dao.entity

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Row
//import  org.apache.spark.implicits._

import scala.collection.immutable.Map
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType



case class ReviewEntity(
     review_id: String,
	   user_id: String, 
	   business_id:String,
	   stars: Float,
	   review_timestamp: Option[Long],
	   review_text: String,
	   useful:Long,
	   funny:Long,
	   cool:Long
	   )
    
    
    
