package app.task
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
import org.apache.spark.sql.functions.{ col, udf, split, ltrim, rtrim, unix_timestamp, when }
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import collection.JavaConversions._
import org.apache.spark.SparkContext
import app.dao.common.CommonUDF
import com.datastax.spark.connector._, org.apache.spark.SparkContext, org.apache.spark.SparkContext._, org.apache.spark.SparkConf

//import scala.language.implicitConversion
//ProcessBusinessInfo
object ProcessTip {
  // val logger: Logger = LogManager.getLogger(ProcessBusinessInfo.getClass)

  def main(args: Array[String]) {

    val logger = LogManager.getRootLogger
    logger.setLevel(Level.WARN)

    //val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").master("local[2]")
      .config("spark.cassandra.connection.host", "127.0.0.1")
      .getOrCreate()

    val path = "/Users/user21/data/tip.json"

    import spark.implicits._

    var input_dataset = spark.read.json(path).select(
      col("user_id"),
      col("compliment_count"), unix_timestamp(
        rtrim(ltrim(col("date"))),
        "yyyy-MM-dd HH:mm:ss").alias("tip_timestamp_seconds"),
      col("business_id"), col("text").alias("tip_text")).withColumn("tip_timestamp", when(col("tip_timestamp_seconds").isNull, null).otherwise(CommonUDF.udf_seconds_to_miliseconds(col("tip_timestamp_seconds"))))
      .as[TipEntity]
    input_dataset.rdd.saveToCassandra("test", "tip",
      SomeColumns(
        "tip_text",
        "tip_timestamp",
        "compliment_count",
        "business_id",
        "user_id"))

    spark.stop()
  }
}