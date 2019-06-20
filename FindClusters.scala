import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
object FindClusters
{


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder.master("local").getOrCreate
    val sc = spark.sparkContext // Just used to create test RDDs

    val filePath = "DataCure"
    //read files
    val files: Array[(Double, Double)] = sc.wholeTextFiles(filePath)
      .flatMap(x=>x._2.split("\n"))
      .map(_.split(","))
      .map(x=>(x(0).toDouble,x(1).toDouble)).sample(false,0.01).collect()


    //create dataset from the array in order to fit the kmeans
    import spark.implicits._
    val allFile: Dataset[(Double, Double)] = spark.createDataset(sc.parallelize(files))

    //create column under name "features"
    val featureCols = Array("_1" , "_2")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val X = assembler.transform(allFile)

    //Train for different number of cluster to find the maximum silhouette
    val numClusters = 10
    for(i <- 2 to numClusters) {
          // Trains a k-means model.
          val kmeans = new KMeans().setK(i).setSeed(1L).setFeaturesCol("features")
          val model = kmeans.fit(X)

          val cost = model.computeCost(X)
          println("The cost is " + cost +"for "+ i +" clusters")

          // Shows the centers of the clusters
          //println("Cluster Centers: ")
          //model.clusterCenters.foreach(println)
          //println()
    }


    sc.stop()
  }
}





