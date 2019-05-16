package app.dao.common
import org.apache.log4j.{ LogManager, Logger }
//import  org.apache.spark.implicits._
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.DataFrame
import app.dao.entity.BusinessInfoEntity
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.functions.{ col, udf, split, when,lit,count,sum,monotonicallyIncreasingId }
import org.apache.spark.sql._
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import collection.JavaConversions._
import org.apache.spark.SparkContext
import com.datastax.spark.connector._, org.apache.spark.SparkContext, org.apache.spark.SparkContext._, org.apache.spark.SparkConf
import org.apache.log4j.{ Level, LogManager, Logger }

object CommonTesting {
  
   val logger =Logger.getLogger("app.dao.common.CommonTesting")
      

  def testExistenceOfRecords(input: List[String], column_name: String, key_space: String, table_name: String, sparkContext: SparkContext): Unit = {
    logger.info(s"******************Testing randomly sampled ${input.length} records existence in table:$table_name")

    val countInCQLTable = sparkContext.cassandraTable(key_space, table_name).where(s"$column_name IN (${
      input.map(a => s"'$a'").mkString(",")
    }) ").spanBy(row => (row.getString(column_name))).count()

    
    if (countInCQLTable != input.length) {
      logger.error(s"******************Failure in: Testing randomly sampled ${input.length} records existence in table:$table_name")
    logger.error(s"******************Failure: count in CQL table:  $countInCQLTable, count in RDD:${input.length}")

    }
    else {
      logger.info(s"******************Success in: Testing randomly sampled ${input.length} records existence in table:$table_name")
    }

  }
  def testTableAndRddCountMatches(inpCount: Long, column_name: String, key_space: String, table_name: String, sparkContext: SparkContext): Unit = {
    logger.info(s"******************Testing whether cardinality of RDD is same as CQL Table's cardinality in table:$table_name grouped by partition key")
    var countOfCQLTable = sparkContext.cassandraTable(key_space, table_name).spanBy(row => (row.getString(column_name))).count()
    if (countOfCQLTable != inpCount) {
      logger.error(s"******************Failure in: Testing whether cardinality of RDD is same as CQL Table's cardinality in table:$table_name")
      logger.error(s"******************Failure in: rdd's cardinality is: $inpCount whereas CQL table $table_name cardinality is $countOfCQLTable")

    } else{
    logger.info(s"******************Success in:Testing whether cardinality of RDD is same as CQL Table's cardinality in table:$table_name")
    }
  }
  def testTable(rddPath: String, key_coumn_name: String, key_space: String, table_name: String, sparkSession: SparkSession): Unit = {
    try {
      var input_rdd= sparkSession.read.json(rddPath)
      var unique_keys_rdd=input_rdd.select(key_coumn_name).groupBy(key_coumn_name).agg(sum(col(key_coumn_name)))
      var keys_of_sample_dataset = unique_keys_rdd.sample(false, 0.2).take(1000)
     var cardinality_of_input_rdd = input_rdd.select(key_coumn_name)
        .groupBy(col(key_coumn_name)).agg(count(col(key_coumn_name))).withColumn("tmp",lit(1))
        .groupBy(col("tmp")).agg(count(col(key_coumn_name)).alias("count")).take(1)(0)
        .getAs[Long]("count") 
      testExistenceOfRecords(
        keys_of_sample_dataset.map(a => a.getAs[String](key_coumn_name)).toList,
        key_coumn_name, key_space, table_name, sparkSession.sparkContext)
     testTableAndRddCountMatches(cardinality_of_input_rdd, key_coumn_name,key_space, table_name, sparkSession.sparkContext)
      
    } catch {
      case e: Exception => {
        e.printStackTrace
        throw e
      }
    }
  }

    def testTableWithDataFrame(df:DataFrame, key_coumn_name: String, key_space: String, table_name: String, sparkSession: SparkSession): Unit = {
    try {
      var input_rdd= df
      var unique_keys_rdd=input_rdd.select(key_coumn_name).groupBy(key_coumn_name).agg(sum(col(key_coumn_name)))
      var keys_of_sample_dataset = unique_keys_rdd.sample(false, 0.2).take(1000)
      var cardinality_of_input_rdd = input_rdd.select(key_coumn_name)
        .groupBy(col(key_coumn_name)).agg(count(col(key_coumn_name))).withColumn("tmp",lit(1))
        .groupBy(col("tmp")).agg(count(col(key_coumn_name)).alias("count")).take(1)(0)
        .getAs[Long]("count") 
      testExistenceOfRecords(
        keys_of_sample_dataset.map(a => a.getAs[String](key_coumn_name)).toList,
        key_coumn_name, key_space, table_name, sparkSession.sparkContext)
     testTableAndRddCountMatches(cardinality_of_input_rdd, key_coumn_name,key_space, table_name, sparkSession.sparkContext)
      
    } catch {
      case e: Exception => {
        e.printStackTrace
        throw e
      }
    }
  }

  
}