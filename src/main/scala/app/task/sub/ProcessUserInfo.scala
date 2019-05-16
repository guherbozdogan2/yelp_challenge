package app.task.sub
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

object ProcessUserInfo {

  def run(spark: SparkSession) {
    try {
           import spark.implicits._
      val path = "../../data/user.json"

      
      var input_dataset = spark.read.json(path).select("*").withColumn(
        "friends_list",
        split(col("friends"), ",")).as[UserEntity]

      input_dataset.rdd.saveToCassandra("test", "user",
        SomeColumns(
          "average_stars",
          "compliment_cool",
          "compliment_cute",
          "compliment_funny",
          "compliment_hot",
          "compliment_list",
          "compliment_more",
          "compliment_note",
          "compliment_photos",
          "compliment_plain",
          "compliment_profile",
          "compliment_writer",
          "cool",
          "elite",
          "fans",
          "friends_list",
          "funny",
          "name",
          "review_count",
          "useful",
          "user_id",
          "yelping_since"))

      spark.stop()
      CommonUDF.successReturn
    } catch {
      case e: Exception => {
        e.printStackTrace
        CommonUDF.failureReturn
      }

    }
  }
}