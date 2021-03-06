package app.task.sub
import org.apache.log4j.{ LogManager, Logger }
//import  org.apache.spark.implicits._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import app.dao.entity.BusinessInfoEntity
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{ col, udf, split, when }
import app.dao.common.CommonUDF
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import collection.JavaConversions._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._, org.apache.spark.SparkContext, org.apache.spark.SparkContext._, org.apache.spark.SparkConf

object ProcessBusinessInfo {

  def run(spark:SparkSession) {

    try {
            import spark.implicits._
      val path = "../../data/business.json"

      var input_dataset = spark.read.json(path).select("*")
        .withColumn("categories", split(col("categories"), ","))
        .withColumn("categories", when(col("categories").isNull, null).otherwise(CommonUDF.udfTrimArrayEntities(col("categories"))))
        .withColumn("attributes_map", when(col("attributes").isNull, null).otherwise(CommonUDF.udfRowToMap(col("attributes"))))
        .withColumn("hours_map", when(col("hours").isNull, null).otherwise(CommonUDF.udfRowToMap(col("hours"))))
        .select("*").as[BusinessInfoEntity]

      input_dataset.rdd.saveToCassandra("test", "business",
        SomeColumns(
          "address", "business_id", "hours_map", "attributes_map", "categories", "city", "is_open", "latitude", "longitude", "name",
          "postal_code", "review_count", "stars", "state"))

      spark.stop()
     
    } catch {
      case e: Exception => {
        e.printStackTrace
        
      }

    }
  }
}