
package com.spotify.jam.apps

import com.spotify.jam.input._
import com.spotify.jam.apps.ReplHelper._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

/** Gets basic details about the dataset*/
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

      // number of unique users
      val numUsers = sc.textFile(fileJams).map(line => {
        // parse jam
        val jamData:Array[String] = Preprocessor.parseJam(line)

        // we required at least jam_id, user_id, artist
        if (jamData.length >= 2)
          // jam_id, artist
          jamData(1)
        else
          ""
      }).distinct().count()
      
      // printing
      printMsg(s"numJams: $numJams, numLikes: $numLikes, numFollowers: $numFollowers")
      printMsg(s"numUsers: $numUsers")
    }

    val help = "Gets basic details about the dataset"

    return new ReplApp(lambda, help)
  }

}
