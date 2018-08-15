import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.math.pow
import org.apache.log4j.{Logger, Level}




object rdd_df {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val session = SparkSession
      .builder
      .appName("Rdds&DataFrames")
      .config("spark.master", "local")
      .getOrCreate()

    import session.implicits._

    val sc = session.sparkContext


    // Create a paired RDD

    val rd = new CreateZipped(10)
    var n = rd.zipped()
    val rdd_ = {sc.parallelize(n, numSlices=4)}

    println(rdd_.mapValues(x => pow(x, 2)).collect().toList)
    println(rdd_.countByKey())
    println(rdd_.foldByKey(zeroValue = 2)((x, y) => x + y).collect().toList)
    println(rdd_.aggregateByKey(zeroValue = 100)(seqOp = (x, y) => x * y, combOp = (x, y) => x + y).collect().toList)
    println(rdd_.getNumPartitions)


    // Turn the RDD into a DataFrame.

    var df = rdd_.toDF("blood_type", "age")

    df = df.withColumnRenamed("blood_type", "BLOOD_TYPE")

    df.select(mean("age")).show()
    df.select(stddev("age")).show()

    df.createOrReplaceTempView("table")
    val query = "SELECT BLOOD_TYPE, AVG(age) AS avg_age FROM table GROUP BY BLOOD_TYPE"
    session.sql(query).show()
  }
}
