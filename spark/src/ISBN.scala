object ISBN {
  def main(args: Array[String]): Unit = {
    println("Welcome To Scala")
    println(isbnMethod("978-1449358624"))
  }
  def isbnMethod(x: String): Any ={
    val x1 = x.toCharArray
    val x2 = x.substring(0,3)+x.substring(4,14)
    var y1 = "";var y2 = "";var y3 = "";var y4 = ""
      y1 = "ISBN" + " " + x.toString
      y2 = "ISBN" + " " + x.substring(0, 3)
      y3 = "ISBN" + " " + x.substring(0, 6)
      y4 = "ISBN" + " " + x.substring(0, 12)
      (y1,y2,y3,y4)
  }
}
    /*if(x.length==14) {y1 = "ISBN"+" "+x.toString}
    y2 = {          val sb = new StringBuilder
      for (a <- 0 until 3) {
        sb.append(x1(a))
      }l
      "ISBN" + " " + sb.toString()}
    y3 = {val sb = new StringBuilder
      for (a <- 0 until 6) {
        sb.append(x1(a))
      }
      "ISBN" + " " + sb.toString()}
    y4 = {          val sb = new StringBuilder
      for (a <- 0 until x1.length - 2) {
        sb.append(x1(a))
      }
      "ISBN" + " " + sb.toString()}*/
