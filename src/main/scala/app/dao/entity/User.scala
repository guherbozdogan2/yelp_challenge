package app.dao.entity

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.Row
//import  org.apache.spark.implicits._

import scala.collection.immutable.Map
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType



case class UserEntity(
     average_stars: Double,
	   compliment_cool: Long, 
	   compliment_cute: Long, 
     compliment_funny: Long, 
     compliment_hot: Long, 
     compliment_list: Long, 
     compliment_more: Long, 
     compliment_note: Long, 
     compliment_photos: Long, 
     compliment_plain: Long, 
     compliment_profile: Long, 
     compliment_writer: Long, 
     cool: Long, 
     elite: String, 
     fans: Long, 
     friends_list:List[String], 
     funny: Long, 
     name:String, 
     review_count: Long, 
     useful: Long, 
     user_id:String, 
     yelping_since:String)
    
    
    
