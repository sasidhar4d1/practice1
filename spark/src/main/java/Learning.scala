import org.apache.spark.{SparkConf, SparkContext}

object WordCount
{
  def main(args: Array[String]) =
  {
    val conf = new SparkConf().setAppName("Word Count").setMaster("local")
    val sc = new SparkContext(conf)

    val dRDD = sc.textFile("/user/cloudera/dataset/word/word")

    val tRDD = dRDD.flatMap(x=>(x.split(' '))).map(x=>(x,1))
      .reduceByKey(_+_)
      .saveAsTextFile("/user/cloudera/dataset/word/output1")
  }
}
