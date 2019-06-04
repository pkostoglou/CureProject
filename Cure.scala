import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

// For implicit conversions like converting RDDs to DataFrames


object Cure {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new spark.SparkConf().setAppName("Cure Application").setMaster("local[*]")
    val sc = new spark.SparkContext(conf)
    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .enableHiveSupport()
      .getOrCreate()

    import sparkSession.implicits._
    import sparkSession.sql

    val allFile = sc.wholeTextFiles("DataCure")
      .flatMap(x=>x._2.split("\n"))
      .map(_.split(","))
      .map(x=>(x(0).toDouble,x(1).toDouble))
    


  }
}