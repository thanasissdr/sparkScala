import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object WordCountStreaming {

  def main(args: Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val session = SparkSession
      .builder
      .appName("WordCount")
      .config("spark.master", "local")
      .getOrCreate()

    import session.implicits._


    val lines = session
      .readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 444)
      .load()

    val words = lines.as[String].flatMap(_.split(" "))
    val wordCount = words.groupBy("value").count().sort(desc("count"))

    val query = wordCount
      .writeStream
      .outputMode("complete")
      .format("console")
      .start()

    query.awaitTermination()

  }
}
