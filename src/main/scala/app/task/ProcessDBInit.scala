
package app.task
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
import com.datastax.driver.core.exceptions.NoHostAvailableException
import com.datastax.spark.connector.cql.CassandraConnector
import scala.io.Source
import java.io.IOException
object ProcessDBInit {
  var wait_time = 1000.0
  var exponentiation_factor = 1.1
  def try_connecting(sparkConf: SparkConf): Unit = {
    try {

      var script_files = List(
        "../../data/db/migration/keyspace.cql",
        "../../data/db/migration/business.cql",
        "../../data/db/migration/checkin.cql",
        "../../data/db/migration/photo.cql",
        "../../data/db/migration/review.cql",
        "../../data/db/migration/tip.cql",
        "../../data/db/migration/user.cql")

      script_files.foreach(f => {
        var scrpt_str = Source.fromFile(f).getLines.mkString(" ")
        CassandraConnector(sparkConf).withSessionDo { session =>
          session.execute(scrpt_str)
        }
      })
      CommonUDF.successReturn
    } catch {
      case ex: java.io.IOException => {
        if (wait_time > 600000) {
          ex.printStackTrace
          CommonUDF.failureReturn
        } else {
          Thread.sleep(wait_time.toInt)
          wait_time = wait_time * exponentiation_factor
          try_connecting(sparkConf)
        }
      }
      case e: Exception => {

        e.printStackTrace
        CommonUDF.failureReturn
      }
    }
  }

  def main(args: Array[String]) {

    val logger = LogManager.getRootLogger
    logger.setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
      .set("spark.cassandra.connection.host", "cassandra")

    try_connecting(conf)
  }
}
    
