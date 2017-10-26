object Pooja {

  def main(args: Array[String]): Unit = {

    def paran(text:String) = {
      val a = text.toArray
      val b =
      { a.count(_.equals('(')) == a.count(_.equals(')')) }
      b
    }

    println(paran(")("))

  }

}
