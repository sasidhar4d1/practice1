import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Dataframe
{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Demo")
    val sc = new SparkContext(conf)
    val hq = new HiveContext(sc)
    val rdd1 = hq.sql("select * from retail_db.categories")
    val rdd2 = rdd1.map(row => {
      val id = row.getInt(0)
      val did = row.getInt(1)
      val name = row.getString(2)
      (name)
    })
    val rdd3 = rdd2.filter(x => (x.startsWith("M"))).coalesce(1).saveAsTextFile("/user/cloudera/Dataframe")
  }
}
