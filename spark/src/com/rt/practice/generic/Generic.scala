package com.rt.practice.generic

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

class Generic
{
  def convertToRDD(inputDF: org.apache.spark.sql.DataFrame) =
  {
    inputDF.rdd
  }
}
