
package com.spotify.jam.apps

import com.spotify.jam.input._
import com.spotify.jam.apps.ReplHelper._
import java.io._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

/**What were the top N most popular jams of all time? 
 * Where the popularity is defined by how often a track is used as a jam. 
 * Let’s assume a track is identified by artist and title. 
 * For each result please provide artist and title. */
object TopJamsApp {

  /** Returns the ReplApp for analyzing the top 5 jams*/
  def makeApp (output:String, sc:SparkContext):ReplApp = {

    // Lambda for processing
    val lambda = (fileJams:String, fileLikes:String, fileFollowers:String, args:Array[String]) => {

      val NUM_JAMS = { 
        if (args.length > 0)
          args(0).toInt
        else
          5
      }
      
      printMsg(s"Computing Top$NUM_JAMS jams" )
      
      // Load jams data
      val rawJams =  sc.textFile(fileJams)

      // creating a PairRDD <(artist, title), 1>
      val jamsMap = rawJams.map(line => {

        // parse jam
        // lower case to clean up data
        val jamData:Array[String] = Preprocessor.parseJam(line.toLowerCase())

        // we required at least jam_id, user_id, artist, title
        if (jamData.length >= 4)
          ((jamData(2) -> jamData(3)), 1)
        else
          (("" -> ""), 1)
      })

      // Performing a "jam count"
      val jamCount = jamsMap.reduceByKey((v1, v2) => v1 + v2) 
     
      // flips the KV: (k,v) => (v, k)
      val topN = jamCount.map({ case (k, v) => (v, k)}) // flips (k,v) => (v, k)
        .top(NUM_JAMS) // pick the top N elements
        .map({ case (v, k) => k._1 + "," + k._2 + ", " + v }).mkString("\n") // creates a string
      // val topN = jamCount.map({ case (k, v) => (v, k)}).groupByKey().top(NUM_JAMS)
            
      // create the output file
      val pw = new PrintWriter(new File(output + "/topJams.csv"))
      pw.write("Artist, Title, Count\n")
      pw.write(topN)
      pw.close()
      printMsg("Artist, Title, Count\n" + topN)
      
    }

    // Help comments
    val help = "What were the top N most popular jams of all time?" + 
      "\ttop_jams <N (optional)>. Where N is 5 by default."

    return new ReplApp(lambda, help)
  }
}
