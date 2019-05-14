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
import org.apache.spark.sql.functions.{ col, udf, split, explode, unix_timestamp, ltrim, rtrim }

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import collection.JavaConversions._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._, org.apache.spark.SparkContext, org.apache.spark.SparkContext._, org.apache.spark.SparkConf

object ProcessCheckin {

  def main(args: Array[String]) {
    try {

      val logger = LogManager.getRootLogger
      logger.setLevel(Level.WARN)

      val spark = SparkSession.builder.appName("Simple Application").master("local[*]")
        .config("spark.cassandra.connection.host", "cassandra")
        .getOrCreate()

      val path = "../../data/checkin.json"

      import spark.implicits._

      var input_dataset = spark.read.json(path).select("*").withColumn(
        "date_list",
        split(col("date"), ","))

        .withColumn("checkin_ts", explode(col("date_list")))
        .withColumn("checkin_ts", ltrim(col("checkin_ts")))
        .withColumn("checkin_ts", rtrim(col("checkin_ts")))
        .filter("checkin_ts is not null and length(checkin_ts)>0")
        .select(col("business_id"), CommonUDF.udfSecondsToMiliseconds(
          unix_timestamp(
            col("checkin_ts"),
            "yyyy-MM-dd HH:mm:ss")).alias("checkin_ts")).filter("checkin_ts is not null")
        .as[CheckinEntity]

      input_dataset.rdd.saveToCassandra("test", "checkin",
        SomeColumns(
          "business_id",
          "checkin_ts"))

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