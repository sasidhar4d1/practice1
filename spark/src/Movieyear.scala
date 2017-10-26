import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

object Movieyear
{
  def main(args: Array[String]): Unit = {
    println("Welocme To Scala")
    val obj = new Movie
  }
}
class Movie{
  val conf = new SparkConf().setAppName("Movie Lens Analysis").setMaster("local")
  val sc = new SparkContext(conf)
  val sq = new SQLContext(sc)
  val dataRDD = sq.read.format("com.databricks.spark.csv").load("/home/cloudera/dataset/movies")
  val t1RDD = dataRDD.select("C0", "C1", "C2").rdd.map(x => {
    val movieid = x.getString(0).toInt
    val moviename = x.getString(1).trim
    val geners = x.getString(2)
    (movieid, moviename, geners)
  })
  val t2RDD = t1RDD.map(x => (x._1, x._3)).flatMapValues(_.split('|'))
  val t3RDD = t1RDD.map(x => (x._1, x._2)).mapValues(x => {
    x.substring(x.length - 5, x.length - 1)
  }).filter(x => (x._2.startsWith("1") || x._2.startsWith("2")))

  val t4RDD = t3RDD.mapValues(x => x.toInt).filter(x => x._2 == 2015).persist(StorageLevel.DISK_ONLY)

  val joinRDD = t4RDD.join(t2RDD).mapValues(x=>x._2).map(x=>(x._2,1))
    .reduceByKey(_+_).map(x=>(x._2,x._1)).sortByKey().max()

  println(joinRDD)
}