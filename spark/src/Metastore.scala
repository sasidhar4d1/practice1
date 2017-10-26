import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import com.rt.practice.generic.Generic

object Metastore
{
  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("Metastore")
    val sc = new SparkContext(conf)
    val sq = new SQLContext(sc)
    val hq = new HiveContext(sc)
    val x = hq.sql("show databases")
    val o = new Generic
    val result = o.convertToRDD(x)
    result.saveAsTextFile("/user/cloudera/metastore100")
  }
}
