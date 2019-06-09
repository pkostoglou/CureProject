import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark

// For implicit conversions like converting RDDs to DataFrames


object Cure {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new spark.SparkConf().setAppName("Cure Application").setMaster("local[*]")
    val sc = new spark.SparkContext(conf)

    val allFile = sc.parallelize(sc.wholeTextFiles("DataCure")
      .flatMap(x=>x._2.split("\n"))
      .map(_.split(","))
      .map(x=>(x(0).toDouble,x(1).toDouble))
      .take(10))

    val scriptPath = "/home/panos/PycharmProjects/MachineLearning/test.py"

    val pipeRDD = allFile.pipe(scriptPath).map(_.split(",")).map(x=>(x(0).toDouble,x(1).toDouble,x(2).toInt))
    pipeRDD.foreach(println)
    print(pipeRDD.count)

  }
}