package day0713

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.distributed.MatrixEntry
import org.apache.spark.mllib.linalg.distributed.CoordinateMatrix
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.SparseVector
/*
 * Build similarity of items to make recommendations
 */
object ItemBasedCF {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //load data
    val conf = new SparkConf().setAppName("UserBaseModel").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.textFile("D:\\download\\data\\ratingdata.txt")

    /*MatrixEntry represents each row in a distributed matrix
     * Each item here is a tuple (I: Long, J: Long, value: Double) indicating the value of the column and column.
     * Where I is the row , j is the column */
    val parseData: RDD[MatrixEntry] =
      data.map(_.split(",") match { case Array(user, item, rate) => MatrixEntry(user.toLong, item.toLong, rate.toDouble) })

    //CoordinateMatrix is the one in Spark MLLib that specifically saves data samples such as user_item_Rating
    val ratings = new CoordinateMatrix(parseData)

    /* Since there is no Columnaccelerating method, we need to convert it to a RowMatrix matrix and call his Columnaccelerating methods to calculate their similarity
     * RowMatrix's method, Columneagerness, calculates how similar the columns are to the columns. Unlike the user-based CF, there is no need to transpose the matrix, just the similarity of the item*/

    val matrix: RowMatrix = ratings.toRowMatrix()

    //Demand: Recommend goods for a certain user. The basic logic is as follows: firstly, we get the goods that have been evaluated (bought) by a certain user, then we calculate the similarity of other goods and rank them.
  
    //For example: recommend a product for User2

    //Step 1: Get the product that user2 has reviewed (bought) take(5) means take out all 5 users 
    //SparseVector£ºSparse matrix
    val user2pred = matrix.rows.take(5)(2)
    val prefs: SparseVector = user2pred.asInstanceOf[SparseVector]
    //get the ID and score of the products that user 2 has evaluated (bought), i.e. :item ID, score
    val uitems = prefs.indices.
	
    val ipi = (uitems zip prefs.values)  


    //Calculate product similarity and output
    val similarities = matrix.columnSimilarities()
    val indexdsimilar = similarities.toIndexedRowMatrix().rows.map {
      case IndexedRow(idx, vector) => (idx.toInt, vector)
    }
//    indexdsimilar.foreach(println)
//    println("*******************")
    
    //ij: The similarity between the goods purchased by other users and those purchased by User2
    val ij = sc.parallelize(ipi).join(indexdsimilar).flatMap {
      case (i, (pi, vector: SparseVector)) => (vector.indices zip vector.values)
    }

    //Ij1: Items and ratings that have been purchased by other users but are not in the list of items purchased by User2
    val ij1 = ij.filter { case (item, pref) => !uitems.contains(item) }
    //ij1.foreach(println)
    //println("*******************")

    //Sum up the ratings of these items, rank them in descending order, and recommend the first two items
    val ij2 = ij1.reduceByKey(_ + _).sortBy(_._2, false).take(2)
    println("********* Recommended results ***********")
    ij2.foreach(println)
  }
}

