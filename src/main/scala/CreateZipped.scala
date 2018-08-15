// Constructor

class CreateZipped(length: Int) {

  def zipped() ={

    var a: Array[Int] = Range(0, length) toArray


    var b: Array[String] = Array("a", "b")
    var c: Array[String] = new Array(length)

    for (i <- 0 to a.length - 1 by 2){

      c(i) = b(0)
      c(i+1) = b(1)
    }

    val zipped_rdd = c zip a
    zipped_rdd.toList
  }
}
