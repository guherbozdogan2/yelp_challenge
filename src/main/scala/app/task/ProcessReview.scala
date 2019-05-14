package app.task
import org.apache.log4j.{ LogManager, Logger }
//import  org.apache.spark.implicits._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import app.dao.entity.ReviewEntity
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{ col, udf, split, ltrim, rtrim, unix_timestamp, when }
import app.dao.common.CommonUDF
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import collection.JavaConversions._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._, org.apache.spark.SparkContext, org.apache.spark.SparkContext._, org.apache.spark.SparkConf

object ProcessReview {

  def main(args: Array[String]) {
    val logger = LogManager.getRootLogger
    logger.setLevel(Level.WARN)

    val spark = SparkSession.builder.appName("Simple Application")
      .getOrCreate()

    val path = "/Users/user21/data/review.json"

    import spark.implicits._

    var input_dataset = spark.read.json(path).select(
      col("review_id"), col("user_id"), col("business_id"), col("stars").cast("float"),
      unix_timestamp(
        rtrim(ltrim(col("date"))),
        "yyyy-MM-dd HH:mm:ss").alias("review_timestamp_seconds"), col("text").alias("review_text"),
      col("useful"), col("funny"), col("cool"))
      .withColumn("review_timestamp", when(col("review_timestamp_seconds").isNull, null).otherwise(CommonUDF.udf_seconds_to_miliseconds(col("review_timestamp_seconds"))))
      .as[ReviewEntity]

    input_dataset.rdd.saveToCassandra("test", "review",
      SomeColumns(
        "review_id",
        "user_id",
        "business_id",
        "stars",
        "review_timestamp",
        "review_text",
        "useful",
        "funny",
        "cool"))

    spark.stop()
  }
}