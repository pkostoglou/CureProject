import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark
import scala.collection.mutable.ArrayBuffer
import scala.math


object Cure {

  //for a point with coordinates (xCor,yCor) finds from the existing representatives the closest and saves their distance
  //later this distance will decide if the point will be a representative or not
  def findRepresentatives(key:Int,xCor:Double,yCor:Double,rep:ArrayBuffer[(Double,Double)]):(Int,((Double,Double),ArrayBuffer[(Double,Double)],Double)) = {
    var min = 10000.0
    for(i<-0 to rep.length-1){
      val dist = math.sqrt(math.pow(rep(i)._1-xCor,2) + math.pow(rep(i)._2-yCor,2))
      if(dist < min){
        min = dist
      }
    }
    return (key,((xCor,yCor),rep,min))
  }
  //from an initial point creates an ArrayBuffer that will hold all representatives for the clusters
  def initRep(key:Int,xCor:Double,yCor:Double):(Int,ArrayBuffer[(Double,Double)]) = {
    val buffer : ArrayBuffer[(Double,Double)] = ArrayBuffer((xCor,yCor))
    return (key,buffer)
  }
  //adds the point with coordinates (xCor,yCor) as a representative for cluster with id key
  def updateRep(key:Int,rep:ArrayBuffer[(Double,Double)],xCor:Double,yCor:Double):(Int,ArrayBuffer[(Double,Double)])={
    val point = (xCor,yCor)
    rep += point
    return (key,rep)
  }

  def updateFinalRep(key:Int,rep:ArrayBuffer[(Double,Double)]):(ArrayBuffer[(Double,Double,Int)])={
    //sumx and sumy will be used to calculate the centroids of the representatives
    var sumx=0.0
    var sumy=0.0
    for(i<- 0 to rep.length -1 ){
      sumx = sumx + rep(i)._1
      sumy = sumy + rep(i)._2
    }
    //initiate the final buffer
    val newBuffer : ArrayBuffer[(Double,Double,Int)]=ArrayBuffer.empty[(Double,Double,Int)]
    val centroid = (sumx/rep.length,sumy/rep.length)
    //moves all the representatives closer to the centroid
    for(y<- 0 to rep.length-1){
      //for a representative we find the line that connects the representative and the centroid then find a new point in this
      //line that will have 20% smaller distance from the centroid in comparison with the initial representative

      //assuming we want to find line y = ax + b we calculate a and b
      val a = (centroid._2 - rep(y)._2)/(centroid._1 - rep(y)._1)
      val b = centroid._2 - a*centroid._1
      var newPoint = (0.0,0.0)
      //calculates the new distance of the new point
      val newDist = 0.8*(math.sqrt(math.pow(centroid._1-rep(y)._1,2)+math.pow(centroid._2-rep(y)._2,2)))
      //this distance represents 2 different points in the line we want the point between the centroid and the initial
      // representative in the report we prove that the next lines are correct
      if(centroid._1 > rep(y)._1){
        val x=centroid._1-math.sqrt(math.pow(newDist,2)/(1+math.pow(a,2)))
        newPoint = (x,a*x+b)
      }else{
        val x=centroid._1+math.sqrt(math.pow(newDist,2)/(1+math.pow(a,2)))
        newPoint = (x,a*x+b)
      }
      //adds the new representative in the buffer with the id of the cluster that it represents
      val p = (newPoint._1,newPoint._2,key)
      newBuffer += p
    }
    return newBuffer
  }
  //finds the right cluster for a point with coordinates (xCor,yCor)
  //finds the closest representative and assigns the point to the respective cluster
  def assignCLusters(rep:ArrayBuffer[(Double,Double,Int)],xCor:Double,yCor:Double):(Int,(Double,Double))={
    var minDist = 1000000.0
    var cluster = -1
    for (i <- 0 to rep.length -1){
      //calculates the distance from the representative
      val dist = math.sqrt(math.pow(rep(i)._1-xCor,2)+math.pow(rep(i)._2-yCor,2))
      //if the distance is smaller updates the variables
      if(dist < minDist){
        minDist = dist
        cluster = rep(i)._3
      }
    }
    return (cluster,(xCor,yCor))
  }

  def main(args: Array[String]): Unit = {
    //Stop showing log messages
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val repNumber = 5
    val numberOfClusters = 5
    val sample = 0.002
    val filePath = "DataCure"
    val scriptPath = "/home/panos/PycharmProjects/MachineLearning/test.py"//the path for the script with the hierarchical algorithm
    val conf = new spark.SparkConf().setAppName("Cure Application").setMaster("local[*]")
    val sc = new spark.SparkContext(conf)

    val allFile =sc.wholeTextFiles(filePath)//read all files in directory
      .flatMap(x=>x._2.split("\n"))//for every file split it's lines
      .map(_.split(","))//separate the 2 coordinates
      .map(x=>(x(0).toDouble,x(1).toDouble,numberOfClusters))//cast the coordinates from string to double

    allFile.persist(StorageLevel.DISK_ONLY)//persist in disk
    val samplePoints = allFile.sample(false,sample)//pick a sample from the whole dataset

    val pipeRDD = samplePoints.pipe(scriptPath)//pass the data for the hierarchical algorithm
      .map(_.split(","))//the results are strings so we follow again the same steps with the file reading process
      .map(x=>(x(2).toInt,(x(0).toDouble,x(1).toDouble)))//the final shape of the rdd is (clusterId,(xCoordinate,yCoordinate))
    //The first representative is picked in random
    var representatives = pipeRDD
      .reduceByKey((a,b)=>(b._1,b._2))
      .map(x=>initRep(x._1,x._2._1,x._2._2))//initialiazation of the representatives their shape is (clusterID,ArrayBuffer(Points))
    representatives.persist(StorageLevel.MEMORY_ONLY)//persist the representatives in memory they will have small size and are used a lot
    for (i<- 1 to repNumber-1){
        val midRDD = pipeRDD.join(representatives)//join the points with their representatives
        //the next step is to find the next representative
        val newRepresentatives = midRDD.map(x=>findRepresentatives(x._1,x._2._1._1,x._2._1._2,x._2._2))//calculate the distances
          // from current representatives
          .reduceByKey((a,b)=> if(a._3 > b._3) a else b)//finds the point that has the highest distance from the representatives
          //for each cluster
        representatives = representatives.join(newRepresentatives) //adds the newly found representative with the previous ones
          .map(x=>updateRep(x._1,x._2._1,x._2._2._1._1,x._2._2._1._2))
    }
    //moves the representatives closer to the center and reduces them to one ArrayBuffer with all the final representatives
    //and the cluster they represent
    val updatedRep = representatives.map(x=>updateFinalRep(x._1,x._2))
      .reduce((a,b)=>a++=b)
    //broadcast the representatives
    val broadBuffer=sc.broadcast(updatedRep)
    //assign a cluster to all the points
    val finalClusters = allFile.map(x=>assignCLusters(broadBuffer.value,x._1,x._2))


    //Evaluation
    //Evaluate the cluster with the sum of squared error technique
    val SSE1 = finalClusters.map(x=>(x._1,(x._2._1,x._2._2,1)))
      .reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,a._3+1))//calculate the sum for all x coorinates, the sum for all y coordinates
      //and lastly the number of points for every cluster
      .mapValues(x=>(x._1/x._3,x._2/x._3))//calculates the centroid for every cluster


    SSE1.foreach(println)
    val SSE =  SSE1.join(finalClusters)//joins all the points with their centroids
      .mapValues(x=>(math.pow(x._1._1-x._2._1,2)+math.pow(x._1._2-x._2._2,2)))//calculate the distance of a point with its centroid
        .reduce((a,b)=>(1,a._2+b._2))._2//calculate SSE
    println(SSE)

  }
}
