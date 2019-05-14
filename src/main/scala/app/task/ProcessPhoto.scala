package app.task
import org.apache.log4j.{ LogManager, Logger }
//import  org.apache.spark.implicits._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import app.dao.entity.PhotoEntity
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{ col, udf, split, ltrim, rtrim, unix_timestamp }

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import collection.JavaConversions._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._, org.apache.spark.SparkContext, org.apache.spark.SparkContext._, org.apache.spark.SparkConf

object ProcessPhoto {

  def main(args: Array[String]) {

    val logger = LogManager.getRootLogger
    logger.setLevel(Level.WARN)

    val spark = SparkSession.builder.appName("Simple Application")
      .getOrCreate()

    val path = "/Users/user21/data/photo.json"

    import spark.implicits._

    var input_dataset = spark.read.json(path).select(
      "photo_id", "business_id", "caption", "label")
      .as[PhotoEntity]

    input_dataset.rdd.saveToCassandra("test", "photo",
      SomeColumns(
        "photo_id", "business_id", "caption", "label"))

    spark.stop()
  }
}