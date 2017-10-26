import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object MovieLens
{
  def main(args: Array[String]): Unit =
  {
    val conf = new SparkConf().setAppName("Movie Lens Analysis").setMaster("local")
    val sc = new SparkContext(conf)
    val sq = new SQLContext(sc)

    val dataRDD = sq.read.format("com.databricks.spark.csv").load("/user/cloudera/movies10")
    val t1RDD = dataRDD.select("C0","C2").rdd.map(x=>{
      val movieid = x.getString(0)
      val geners = x.getString(1)
      (movieid,geners)
    })

    val t3RDD = t1RDD.map(x=>(x._1.toInt,x._2)).flatMapValues(x=>(x.split('|'))).map(x=>(x._2,x._1))
      .mapValues(x=>(x,1)).mapValues(x=>x._2)

    val result = t3RDD.reduceByKey(_+_).map(x=>(x._2,x._1))
      .sortByKey().saveAsTextFile("/user/cloudera/output100")
  }
}