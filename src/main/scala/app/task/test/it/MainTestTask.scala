package app.task.test.it
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
import app.task.test.it.sub._

import org.apache.log4j.{ Level, Logger }
import scala.util.{Try,Success,Failure}


object MainTestTask {
 val logger = Logger.getLogger("app.task.test.it.MainTestTask")
  def main(args: Array[String]) {
    
    try {
      val spark = SparkSession.builder.appName("Simple Application").master("local[*]")
        .config("spark.cassandra.connection.host", "cassandra")
        .getOrCreate()

        
        Logger.getRootLogger().setLevel(Level.ERROR)
        Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
        Logger.getLogger("org.spark-project").setLevel(Level.ERROR)
        Logger.getLogger("org").setLevel(Level.ERROR)
        Logger.getLogger("akka").setLevel(Level.ERROR)
        Logger.getLogger("app.task.test.it.MainTestTask").setLevel(Level.INFO)
        Logger.getLogger("app.dao.common.CommonTesting").setLevel(Level.INFO)

      import spark.implicits._

      logger.info(s"Starting testing tables")
          Try(ProcessBusinessInfo.run(spark)) match {
            case Success(_) =>  logger.info(s"Test of table:business succeeded")
            case Failure(_) => logger.error(s"Failure in testing table:business")
          }
          
          logger.info(s"Starting testing table checkin")
          Try(ProcessCheckin.run(spark)) match {
            case Success(_) =>  logger.info(s"Test of table:checkin succeeded")
            case Failure(_) => logger.error(s"Failure in testing table:checkin")
          }
          
          logger.info(s"Starting testing table photo")
           Try(ProcessPhoto.run(spark)) match {
            case Success(_) =>  logger.info(s"Test of table:photo succeeded")
            case Failure(_) => logger.error(s"Failure in testing table:photo")
          }
          
           logger.info(s"Starting testing table review")
            Try(ProcessReview.run(spark)) match {
            case Success(_) =>  logger.info(s"Test of table:review succeeded")
            case Failure(_) => logger.error(s"Failure in testing table:review")
          }
            
           logger.info(s"Starting testing table tip")
             Try(ProcessTip.run(spark)) match {
            case Success(_) =>  logger.info(s"Test of table:tip succeeded")
            case Failure(_) => logger.error(s"Failure in testing table:tip")
          }
             
           logger.info(s"Starting testing table user")
              Try(ProcessUserInfo.run(spark)) match {
            case Success(_) =>  logger.info(s"Test of table:user succeeded")
            case Failure(_) => logger.error(s"Failure in testing table:user")
          }
       
          logger.info(s"Tests ended")
          
          

      spark.stop()
    } catch {
      case e: Exception => {
        e.printStackTrace
      }

    }
  }
}