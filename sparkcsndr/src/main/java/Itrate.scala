import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql._
import org.apache.spark.sql.SQLContext


object Itrate {


  def main(args : Array[String]): Unit =
  {


         val conf = new SparkConf()
        .set("spark.cassandra.connection.host", "localhost")
        .set("spark.cassandra.auth.username", "cassandra")
        .set("spark.cassandra.auth.password", "cassandra")
        .setMaster("local[2]")
        .setAppName("spark-cassendra")

    val sc = new SparkContext(conf)

   val sqlContext = new SQLContext(sc)

   val df = sqlContext
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> "emp", "keyspace" -> "users")).load.cache()



  }

}
