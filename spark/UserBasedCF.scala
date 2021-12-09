package day0713

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.spark.mllib.linalg.distributed.RowMatrix


/*
 * 1¡¢Build the similarity matrix of users
 * 2¡¢Calculate data
 */
object UserBasedCF {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)   
    
    //Creat a SparkContext
    val conf = new SparkConf().setAppName("BlackUserList").setMaster("local")
    val sc = new SparkContext(conf)
    
    //load data
    val data = sc.textFile("D:\\download\\data\\ratingdata.txt")
    //MatrixEntry£ºA row in a matrix
    //Using pattern match
    val parseData:RDD[MatrixEntry] = data.map(_.split(",") 
        match {case Array(user,item,rate) => MatrixEntry(user.toLong,item.toLong,rate.toDouble)} )
        
    //Construct score matrix new CoordinateMatrix(entries: RDD[MatrixEntry]) 
    val ratings = new CoordinateMatrix(parseData)
    
    //Compute the user's similarity matrix: the matrix transpose is required
    val matrix:RowMatrix = ratings.transpose().toRowMatrix()
    
    //The similarity matrix of users is obtained by calculation
    val similarities = matrix.columnSimilarities()
    println("Output user similarity matrix")
    similarities.entries.collect().map(x=>{
      println(x.i +" --->" + x.j+ "  ----> " + x.value)
    })
    
    println("-----------------------------------------")
    //Get a user's rating of all items: User 1 for example
    val ratingOfUser1 = ratings.entries.filter(_.i == 1).map(x=>(x.j,x.value)).sortBy(_._1).collect().map(_._2).toList.toArray
    println("ratingOfUser1")
    for(s<-ratingOfUser1) println(s)
    
    println("-----------------------------------------")
    
    //Get the similarity of user1 relative to other users
    val similarityOfUser1 = similarities.entries.filter(_.i == 1).sortBy(_.value,false).map(_.value).collect
    println("similarity of user1 relative to other users")
    for(s<- similarityOfUser1) println(s)
      

    sc.stop()
  }
}



















