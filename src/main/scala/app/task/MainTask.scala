package app.task
import org.apache.log4j.{ LogManager, Logger }
//import  org.apache.spark.implicits._
import org.apache.spark.{ SparkConf, SparkContext }

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import app.dao.entity.CheckinEntity
import com.datastax.spark.connector._
import app.dao.common.CommonUDF
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{ col, udf, split, explode, unix_timestamp, ltrim, rtrim, monotonicallyIncreasingId }

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import collection.JavaConversions._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._, org.apache.spark.SparkContext, org.apache.spark.SparkContext._, org.apache.spark.SparkConf
import app.task.sub._
import scala.util.{Try,Success,Failure}

object MainTask {

  def main(args: Array[String]) {
    val logger = Logger.getLogger("app.task.MainTask")
    try {
      val spark = SparkSession.builder.appName("Simple Application").master("local[*]")
        .config("spark.cassandra.connection.host", "cassandra")
        .getOrCreate()
        
        Logger.getRootLogger().setLevel(Level.ERROR)
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
        Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        Logger.getLogger("app.task.MainTask").setLevel(Level.INFO)

     
      import spark.implicits._
      
      logger.info(s"Starting DB tables creation")
      Try(ProcessDBInit.run(spark)) match {

        case Success(_) =>  { 
          
          logger.info(s"DB tables creation succeeded")
          //starting importing tables
          
          logger.info(s"Starting importing data to table business")
          Try(ProcessBusinessInfo.run(spark)) match {
            case Success(_) =>  logger.info(s"Importing data to table:business succeeded")
            case Failure(_) => logger.error(s"Failed to import data table:table:business")
          }
          
          logger.info(s"Starting importing data to table checkin")
          Try(ProcessCheckin.run(spark)) match {
            case Success(_) =>  logger.info(s"Importing data to table:checkin succeeded")
            case Failure(_) => logger.error(s"Failed to import data table:checkin")
          }
          
          logger.info(s"Starting importing data to table photo")
           Try(ProcessPhoto.run(spark)) match {
            case Success(_) =>  logger.info(s"Importing data to table:photo succeeded")
            case Failure(_) => logger.error(s"Failed to import data table:photo")
          }
          
           logger.info(s"Starting importing data to table review")
            Try(ProcessReview.run(spark)) match {
            case Success(_) =>  logger.info(s"Importing data to table:review succeeded")
            case Failure(_) => logger.error(s"Failed to import data table:review")
          }
            
           logger.info(s"Starting importing data to table tip")
             Try(ProcessTip.run(spark)) match {
            case Success(_) =>  logger.info(s"Importing data to table:tip succeeded")
            case Failure(_) => logger.error(s"Failed to import data table:tip")
          }
             
           logger.info(s"Starting importing data to table user")
              Try(ProcessUserInfo.run(spark)) match {
            case Success(_) =>  logger.info(s"Importing data to table:user succeeded")
            case Failure(_) => logger.error(s"Failed to import data table:user")
          }
//        
          logger.info(s"Importing data to tables ended.")
        }
        
        
        case Failure(_) => logger.error(s"Failed to create DB tables")
   
      }
 

      spark.stop()
    } catch {
      case e: Exception => {
        e.printStackTrace
      }

    }
  }
}