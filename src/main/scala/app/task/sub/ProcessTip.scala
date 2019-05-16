package app.task.sub
import org.apache.log4j.{ LogManager, Logger }
//import  org.apache.spark.implicits._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import app.dao.entity.TipEntity
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{ col, udf, split, ltrim, rtrim, unix_timestamp, when,monotonicallyIncreasingId }
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import collection.JavaConversions._
import org.apache.spark.SparkContext
import app.dao.common.CommonUDF
import com.datastax.spark.connector._, org.apache.spark.SparkContext, org.apache.spark.SparkContext._, org.apache.spark.SparkConf

object ProcessTip {

  def run(spark:SparkSession) {
    try {
           import spark.implicits._

      val path = "../../data/tip.json"

     
      var input_dataset = spark.read.json(path).select(
        col("user_id"),
        col("compliment_count"), unix_timestamp(
          rtrim(ltrim(col("date"))),
          "yyyy-MM-dd HH:mm:ss").alias("tip_timestamp_seconds"),
        col("business_id"), col("text").alias("tip_text"))
        .filter("tip_timestamp_seconds is not null")
        .withColumn("tip_timestamp", CommonUDF.udfSecondsToMiliseconds(col("tip_timestamp_seconds")))
         .withColumn("tip_id", monotonicallyIncreasingId)
        .as[TipEntity]
      //logger.info(s"******************input dataset's cardinality is ${input_dataset.count()}")
      input_dataset.rdd.saveToCassandra("test", "tip",
        SomeColumns(
          "tip_text",
          "tip_id",
          "tip_timestamp",
          "compliment_count",
          "business_id",
          "user_id"))

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