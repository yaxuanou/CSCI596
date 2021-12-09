package day0713

import org.apache.spark.mllib.recommendation.ALS
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.recommendation.Rating
import scala.io.Source
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

object ALSDemo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //Read the data and convert it to RDD[Rating] to get the scoring data
    val conf = new SparkConf().setAppName("UserBaseModel").setMaster("local")
    val sc = new SparkContext(conf)
    val productRatings = loadRatingData("D:\\download\\data\\ratingdata.txt")
    val prodcutRatingsRDD:RDD[Rating] = sc.parallelize(productRatings)
    
    //output
      val numRatings = prodcutRatingsRDD.count
//    val numUsers = prodcutRatingsRDD.map(x=>x.user).distinct().count
//    val numProducts = prodcutRatingsRDD.map(x=>x.product).distinct().count
//    println("ratings£º" + numRatings +"\t users£º" + numUsers +"\t products£º"+ numProducts)
 
    /*View the ALS training model API
        ALS.train(ratings, rank, iterations, lambda)
	Parameters:
	ratings - RDD of (userID, productID, rating) pairs
	rank - number of features to use
	       Suggestion: between 10 and 200
	       The larger the rank, the more accurate the split
	       The smaller the rank, the faster the speed
	iterations - number of iterations of ALS (recommended: 10-20)		             
	lambda - regularization factor 
		The higher the value, the more severe the regularization process;
		If the value is smaller, the more accurate, use 0.01	             
			      
    */    
    //val model = ALS.train(prodcutRatingsRDD, 50, 10, 0.01)
    val model = ALS.train(prodcutRatingsRDD, 10, 5, 0.5)
    val rmse = computeRMSE(model,prodcutRatingsRDD,numRatings)
    println("error£º" + rmse)
    
    
    //Use this model to make recommendations
    //Demand: Recommend 2 products to user 1                                     
    val recomm = model.recommendProducts(1, 2)
    recomm.foreach(r=>{ 
      println("user£º" + r.user.toString() +"\t product£º"+r.product.toString()+"\t rating:"+r.rating.toString())
    })    
    
    sc.stop()
    
  }
  
    //compute RMSE £º Root Mean Square Error
  def computeRMSE(model: MatrixFactorizationModel, data: RDD[Rating], n: Long): Double = {
    val predictions: RDD[Rating] = model.predict((data.map(x => (x.user, x.product))))
    val predictionsAndRating = predictions.map {
      x => ((x.user, x.product), x.rating)
    }.join(data.map(x => ((x.user, x.product), x.rating))).values

    math.sqrt(predictionsAndRating.map(x => (x._1 - x._2) * (x._1 - x._2)).reduce(_ + _) / n)

  }
  
  
  
  
  //load data
  def loadRatingData(path:String):Seq[Rating] = {
    val lines = Source.fromFile(path).getLines()
    
    //Filter out data with a score of 0
    val ratings = lines.map(line=>{
        val fields = line.split(",")
        //return rating object: userid productid ratings data
        Rating(fields(0).toInt,fields(1).toInt,fields(2).toDouble)
    }).filter(x => x.rating > 0.0)
    
    //covert to Seq[Rating]
    if(ratings.isEmpty){
      sys.error("Error ....")
    }else{
      //return  Seq[Rating]
      ratings.toSeq
    }
    
  }
}
