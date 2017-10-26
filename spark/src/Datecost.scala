import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object Datecost
{
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Date Cost Analysis").setMaster("local")
    val sc = new SparkContext(conf)
    val sq = new SQLContext(sc)

    val input = sc.textFile("/home/cloudera/dataset/datecost")
      .map(x=>(x.split(',')(0),x.split(',')(1)))

    val x = sq.read.format("com.databricks.spark.csv").load("/home/cloudera/dataset/datecost")
      .registerTempTable("Account")

    val x1 = sq.sql("select C0, TO_DATE(C1) as C1 from Account").registerTempTable("Temp1")

    val x2 = sq.sql("select C0, max(C1) as two, min(C1) as one from Temp1 group by C0").registerTempTable("Temp2")

    val x3 = sq.sql("select C0, DATEDIFF(two,one) as C1 from Temp2").registerTempTable("Temp3")

    val x4 = sq.sql("select C0, C1*0.5 from Temp3")

    val x5 = sq.sql("select C0, C1*0.5 from " +
      "(select C0, DATEDIFF(two,one) as C1 from " +
      "(select C0, max(C1) as two, min(C1) as one from " +
      "(select C0, TO_DATE(C1) as C1 from Account) group by C0))")

    x5.foreach(println(_))

  }
}
