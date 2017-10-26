package com.practice.dataframe
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

object Greatest3
{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val hq = new HiveContext(sc)
    val sq = new SQLContext(sc)
    import sq.implicits._

    val firstRDD = sq.read.orc("")
      }
}
