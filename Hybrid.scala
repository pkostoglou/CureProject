import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer



object Hybrid {


  //Calculates the euclidean distance between two points
  def euclideanDistance( x1:Double, y1:Double, x2:Double, y2:Double ) : Double = {
    var dist:Double = Math.sqrt( (y2-y1)*(y2-y1) + (x2-x1)*(x2-x1) )
    return dist
  }

  //function used to find the cluster of a point and calculate the distance from it's center
  def calcDist(centers: ArrayBuffer[(Double,Double,Long)],x: Double,y:Double):(Long,Double)={
    var currentCenter = -1.toLong
    var minDist = 10000000.0
    //we assign the point to the cluster that it's center is closer to the point
    for (i<- 0 to centers.length-1){
      if(euclideanDistance(centers(i)._1,centers(i)._2,x,y) < minDist){
        minDist = euclideanDistance(centers(i)._1,centers(i)._2,x,y)
        currentCenter = centers(i)._3
      }
    }
    //return the clusterId and the distance squared
    return (currentCenter,math.pow(minDist,2))
  }


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val spark = SparkSession.builder.master("local").getOrCreate
    val sc = spark.sparkContext // Just used to create test RDDs

    val filePath = "DataCure"
    val numberOfClusters = 5

    val allFile = sc.wholeTextFiles(filePath)
      .flatMap(x=>x._2.split("\n"))
      .map(_.split(","))
      .map(x=>(x(0).toDouble,x(1).toDouble))


    /////////////////////// K MEANS ////////////////////////
    // Cluster the data into classes using KMeans with RDDs

    val numClusters = numberOfClusters*5     //number of clusters for kmeans
    val numIterations = 1000  //number of iterations

    //transform RDD into RDD[linalg.Vector]
    val parsedData = allFile.map(s => Vectors.dense(s._1, s._2))


    //train the kmeans with the given data for the number of clusters
    val clusters = KMeans.train(parsedData, numClusters, numIterations, "random")

    //find the coordinates of the centers of every cluster
    val centers = clusters.clusterCenters


    //Transform centers from Array[linalg.Vector] into RDD[(Double, Double)]
    val input_centers = sc.parallelize(centers.map(x => (x(0), x(1))))

    //predict the cluster every instance belongs to
    val predictData = clusters.predict(parsedData)



    ////////////////// AGGLOMERATIVE HIERARCHICAL CLUSTERING /////////////////////

    //transform the coordinates of the centers into RDD[((Double, Double), Long)] where
    // (Double, Double) are the coordinates and Long is the index of the cluster.
    // Here we use it as an ID in order to know which clusters to merge
    var hClusters = input_centers.zipWithIndex()

    //We make a var copy of the clusters every point belongs to in order to change it later
    var hcData = predictData

    //define the number of final clusters of the hierarchical algorithm
    val numClustersHC = numberOfClusters

    //in every iteration of this loop, the algorithm finds the centers with
    //the minimum euclidean distance and merges them together
    for(i <- 0 to (numClusters-numClustersHC-1)){
      //gets the RDD and calculates the distance between every pair of coordinates
      //as input it gets the x coordinates of the first center (x1, y1)
      //as well as the coordinates of the second center (x2, y2)
      //and returns a nxn array of doubles with every distance between every pair
      val rdd2 = hClusters
      val distance = rdd2.cartesian(rdd2)
        .map(x => (euclideanDistance(x._1._1._1, x._1._1._2, x._2._1._1, x._2._1._2), x._1._2, x._2._2 ))


      //among all we computed the distance between the same center and that result is 0.0
      //we filter this value and get the minimum distance between the others
      //and minDistance is a (Double, Long, Long) where Double is the distance and Long,Long the two points
      val minDistance = distance.filter(_._1 > 0.0).min


      //n1 is the one center need to be merged and n2 is the other
      val n1 = minDistance._2
      val n2 = minDistance._3
      val minD = (n1,n2)

      //Then, we filter the hClusters in order to have the coordinates
      //of the two centers we want to merge.
      val p1 = hClusters.filter(x =>  (x._2 == minD._1)).take(1)(0)._1
      val p2 = hClusters.filter(x =>  (x._2 == minD._2)).take(1)(0)._1

      //and we calculate the average distance between them
      val p = ( (p1._1 + p2._1) / 2.0 , (p1._2 + p2._2) / 2.0)

      //After finding the mean distance, we delete the second center
      hClusters = hClusters.filter(x => x._2 != minD._2 )

      //and update the first one with the mean distance as its new center
      hClusters = hClusters.map(x => if(x._2 == minD._1) (p, x._2) else x)

      //Finally we update the initial data with their final cluster
      hcData = hcData.map(x => if(x == minD._2.toInt) minD._1.toInt else x)

    }
    hClusters.foreach(println)
    ////////////////// EVALUATION /////////////////////
    //Save the centers in an ArrayBuffer so we can broadcast them
    val p =hClusters.map(x=>(ArrayBuffer((x._1._1,x._1._2,x._2))))
      .reduce((a,b)=>a++=b)
    val broadCenters = sc.broadcast(p)

    //For every point calculate the distance from it's center
    val SSE = allFile.map(x=>calcDist(broadCenters.value,x._1,x._2))
      .reduce((a,b)=>(1,a._2+b._2))._2//add all the distances together
    print("The sum of squared errors is "+ SSE)
  }
}





