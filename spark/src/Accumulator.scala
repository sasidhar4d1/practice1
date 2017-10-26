import org.apache.spark.{SparkConf, SparkContext}

object Accumulator
{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Accumulator").setMaster("local")
    val sc = new SparkContext(conf)
    val inputfile = sc.textFile("/home/cloudera/dataset/data1/ratings1.csv")
    var blanklines = sc.accumulator(0)
    //var blanklines = 0
    val result = inputfile.foreach(x=>{
      if(x.length==0) blanklines+=1
    })
    println(blanklines.value)
  }
}
