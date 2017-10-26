import java.sql.{DriverManager, ResultSet}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

object MYSQLImport
{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("MYSQL").setMaster("local")
    val sc = new SparkContext(conf)
    val sq = new HiveContext(sc)
    val connection = "jdbc:mysql://localhost"
    val databasename = "retail_db"
    val sourcetable = "orders"
    val username = "root"
    val password = "cloudera"
    val properties = new java.util.Properties()
    properties.setProperty("user",username)
    properties.setProperty("password",password)
    val ordersDF = sq.read.jdbc(connection+"/"+databasename,sourcetable,properties)
    ordersDF.registerTempTable("orders")
    //sq.sql("create table if not exists orders_hive as select * from orders")
    sq.sql("create table if not exists orders_temp as select * from orders_hive where 1 = 2")
    sq.sql("insert overwrite table orders_temp select t1.* from orders t1 join (select MAX(order_id) as max_dt from orders_max) t2 where t1.order_id > t2.max_dt")
    sq.sql("insert into table orders_hive select * from orders_temp")
    sq.sql("create table if not exists orders_max (order_id int)")
    sq.sql("insert overwrite table orders_max select MAX(order_id) from orders")
  }
}
