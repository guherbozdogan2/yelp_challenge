package app.task.test.it.sub
import org.apache.log4j.{ LogManager, Logger }
//import  org.apache.spark.implicits._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import app.dao.entity.UserEntity
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{ col, udf, split }
import app.dao.common.CommonUDF
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import collection.JavaConversions._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._, org.apache.spark.SparkContext, org.apache.spark.SparkContext._, org.apache.spark.SparkConf


import app.dao.common.CommonTesting

object ProcessUserInfo {

  def run(spark: SparkSession) {

    try {
      import spark.implicits._
      CommonTesting.testTable("../../data/user.json", "user_id", "test", "user", spark)
      spark.stop()
    } catch {
      case e: Exception => {
        e.printStackTrace
       
      }

    }
  }
}