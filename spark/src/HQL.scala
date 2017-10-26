import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._

object HQL
{
  def main(args: Array[String]): Unit = {
    val inputpath = args(0)
    val conf = new SparkConf().setAppName("HQL").setMaster("local")
    val sc = new SparkContext(conf)
    val sq = new SQLContext(sc)
    val hq = new HiveContext(sc)
    import hq.implicits._
/*    val schema1 = StructType(Array(StructField("user_id",IntegerType,true),
      StructField("movie_id",IntegerType,true),
      StructField("rating",DoubleType,true),
      StructField("time",IntegerType,true)))*/
    val input = sc.textFile(inputpath)
    val input1 = input.filter(x=>x.length>0).map(x=>x.split(','))
    //val input2 = hq.createDataFrame(input1,schema1)
    val input2 = input1.map(x=>rate_df(x(0).toInt,x(1).toInt,x(2).toDouble,x(3).toInt))
    val input3 = input2.toDF().registerTempTable("movie_rating")
    val result = hq.sql("select * from movie_rating")
    result.foreach(println(_))
  }
}
case class rate_df(user_id: Int, movie_id: Int, rating: Double, time: Int)
