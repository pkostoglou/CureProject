import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark
import scala.collection.mutable.ArrayBuffer
import scala.math


object Cure {

  val repNumber = 4

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
  def initRep(key:Int,xCor:Double,yCor:Double):(Int,ArrayBuffer[(Double,Double)]) = {
    val buffer : ArrayBuffer[(Double,Double)] = ArrayBuffer((xCor,yCor))
    return (key,buffer)
  }
  def updateRep(key:Int,rep:ArrayBuffer[(Double,Double)],xCor:Double,yCor:Double):(Int,ArrayBuffer[(Double,Double)])={
    val point = (xCor,yCor)
    rep += point
    return (key,rep)
  }
  def updateFinalRep(key:Int,rep:ArrayBuffer[(Double,Double)]):(ArrayBuffer[(Double,Double,Int)])={
    var sumx=0.0
    var sumy=0.0
    for(i<- 0 to rep.length -1 ){
      sumx = sumx + rep(i)._1
      sumy = sumy + rep(i)._2
    }
    val newBuffer : ArrayBuffer[(Double,Double,Int)]=ArrayBuffer.empty[(Double,Double,Int)]
    val centroid = (sumx/rep.length,sumy/rep.length)
    for(y<- 0 to rep.length-1){
      val a = (centroid._2 - rep(y)._2)/(centroid._1 - rep(y)._1)
      val b = centroid._2 - a*centroid._1
      var newPoint = (0.0,0.0)
      val newDist = 0.8*(math.sqrt(math.pow(centroid._1-rep(y)._1,2)+math.pow(centroid._2-rep(y)._2,2)))
      if(centroid._1 > rep(y)._1){
        val x=centroid._1-math.sqrt(math.pow(newDist,2)/(1+math.pow(a,2)))
        newPoint = (x,a*x+b)
      }else{
        val x=centroid._1+math.sqrt(math.pow(newDist,2)/(1+math.pow(a,2)))
        newPoint = (x,a*x+b)
      }
      val p = (newPoint._1,newPoint._2,key)
      newBuffer += p
    }
    return newBuffer
  }

  def assignCLusters(rep:ArrayBuffer[(Double,Double,Int)],xCor:Double,yCor:Double):(Int,(Double,Double))={
    var minDist = 1000000.0
    var cluster = -1
    for (i <- 0 to rep.length -1){
      val dist = math.sqrt(math.pow(rep(i)._1-xCor,2)+math.pow(rep(i)._2-yCor,2))
      if(dist < minDist){
        minDist = dist
        cluster = rep(i)._3
      }
    }
    return (cluster,(xCor,yCor))
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val conf = new spark.SparkConf().setAppName("Cure Application").setMaster("local[*]")
    val sc = new spark.SparkContext(conf)

    val allFile =sc.wholeTextFiles("DataCure")
      .flatMap(x=>x._2.split("\n"))
      .map(_.split(","))
      .map(x=>(x(0).toDouble,x(1).toDouble))

    allFile.persist(StorageLevel.DISK_ONLY)
    val samplePoints = allFile.sample(false,0.005)
    val scriptPath = "/home/panos/PycharmProjects/MachineLearning/test.py"

    val pipeRDD = samplePoints.pipe(scriptPath).map(_.split(",")).map(x=>(x(2).toInt,(x(0).toDouble,x(1).toDouble)))
    var representatives = pipeRDD.reduceByKey((a,b)=>(b._1,b._2)).map(x=>initRep(x._1,x._2._1,x._2._2))
    representatives.persist(StorageLevel.MEMORY_ONLY)
    for (i<- 1 to repNumber-1){
        val midRDD = pipeRDD.join(representatives)
        val newRepresentatives = midRDD.map(x=>findRepresentatives(x._1,x._2._1._1,x._2._1._2,x._2._2))
          .reduceByKey((a,b)=> if(a._3 > b._3) a else b)
        representatives = representatives.join(newRepresentatives)
          .map(x=>updateRep(x._1,x._2._1,x._2._2._1._1,x._2._2._1._2))
    }
    val updatedRep = representatives.map(x=>updateFinalRep(x._1,x._2))
      .reduce((a,b)=>a++=b)
    val broadBuffer=sc.broadcast(updatedRep)
    val finalClusters = allFile.map(x=>assignCLusters(broadBuffer.value,x._1,x._2))


    //Evaluation
    val SSE = finalClusters.map(x=>(x._1,(x._2._1,x._2._2,1)))
      .reduceByKey((a,b)=>(a._1+b._1,a._2+b._2,a._3+1))
      .mapValues(x=>(x._1/x._3,x._2/x._3))
      .join(finalClusters)
      .mapValues(x=>(math.pow(x._1._1-x._2._1,2)+math.pow(x._1._2-x._2._2,2)))
      .reduce((a,b)=>(1,a._2+b._2))._2
    println(SSE)

  }
}