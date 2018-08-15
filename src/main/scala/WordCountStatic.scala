import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object WordCountStatic {

  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)

    val session = SparkSession
      .builder
      .appName("WordCountStatic")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = session.sparkContext
    val textFile = sc.textFile("src/main/resources/shakespeare.txt")

    val wordCount = textFile
      .flatMap(_.split(" "))
      .map(w => (w, 1))
      .reduceByKey(_ + _)

    wordCount.foreach(println)
  }
}
