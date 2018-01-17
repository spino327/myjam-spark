
package com.spotify.jam.apps

import com.spotify.jam.apps.ReplHelper._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

/** */
object BasicApp {
  
  def makeApp (output:String, sc:SparkContext) : ReplApp = {

    // lambda function to process the data
    val lambda = (fileJams:String, fileLikes:String, fileFollowers:String, args:Array[String]) => {

      // number of jams
      val numJams = sc.textFile(fileJams).count()

      // number of likes
      val numLikes = sc.textFile(fileLikes).count()
      
      // number of followers
      val numFollowers = sc.textFile(fileFollowers).count()

      // printing
      printMsg(s"numJams: $numJams, numLikes: $numLikes, numFollowers: $numFollowers")
    }

    val help = "Computes number of jams, likes, and followers in the input dataset"

    return new ReplApp(lambda, help)
  }

}
