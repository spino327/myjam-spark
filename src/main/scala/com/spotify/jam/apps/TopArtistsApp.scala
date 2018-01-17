
package com.spotify.jam.apps

import com.spotify.jam.input._
import com.spotify.jam.apps.ReplHelper._
import java.io._
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

/**
 * What were the top N most liked artists. 
 * An artist is liked if a jam by that artists was liked by a user.
 */
object TopArtistsApp {

  /** Returns the ReplApp for analyzing the top N artists*/
  def makeApp (output:String, sc:SparkContext):ReplApp = {

    // Lambda for processing
    val lambda = (fileJams:String, fileLikes:String, fileFollowers:String, args:Array[String]) => {

      val NUM_ART = { 
        if (args.length > 0)
          args(0).toInt
        else
          5
      }
      
      ReplHelper.printMsg(s"Computing Top$NUM_ART artists" )
      
      /*
       * Computing LikesCount <jam_id, #likes> from Likes file
       */
      // Load Likes data
      val rawLikes =  sc.textFile(fileLikes)

      // creating a PairRDD <(jam_id, #likes)>
      val jamLikesCount = rawLikes.map(line => {

        // parse Like
        val likeData:Array[String] = Preprocessor.parseLike(line)

        // we required at least (user_id, jam_id)
        if (likeData.length >= 2)
          (likeData(1) -> 1)
        else
          ("" -> 1)
      }).reduceByKey((v1, v2) => v1+v2)
      
      /*
       * Computing <jam_id, artist> from Jams file
       */
      // Load jams data
      val rawJams =  sc.textFile(fileJams)

      // creating a PairRDD <(jam_if, artist)>
      val jamArtist = rawJams.map(line => {

        // parse jam
        val jamData:Array[String] = Preprocessor.parseJam(line)

        // we required at least jam_id, user_id, artist
        if (jamData.length >= 3)
          // jam_id, artist
          (jamData(0) -> jamData(2))
        else
          ("" -> "")
      })
      
      /*
       * Joining jamArtist + jamLikesCount to get <jam_id, artist, #likes>
       * We do it by performing a natural join since we don't care for those
       * jams that don't have likes
       */
      // PairRDD[jam_id, [artist, #likes]]
      val jamArtistLikes = jamArtist.join(jamLikesCount)
      
      /*
       * Computing top <artist, #likes>
       */
      // performing likes count per artist <artist, #likes> from PairRDD[jam_id, [artist, #likes]]
      val artistLikes = jamArtistLikes.map({ 
        // creating RDD <artist, #likes>
        case (jam_id, artist_likes) => (artist_likes._1 -> artist_likes._2)
      }).reduceByKey((v1, v2) => v1 + v2) 
      
      val topN = artistLikes.map({ case (artist, n_likes) => (n_likes, artist)}) // flips
        .top(NUM_ART) // pick top N
        .map({ case (v, k) => s"$k, $v"}).mkString("\n") // creates string of results
        
      // create the output file
      val pw = new PrintWriter(new File(output + "/topArtists.csv"))
      pw.write("Artist, #Likes\n")
      pw.write(topN)
      pw.close()
      println("Artist, #Likes\n" + topN)
      
    }

    // Help comments
    val help = "What were the top N most liked artists?" + 
      "\ttop_arts <N (optional)>. Where N is 5 by default."

    return new ReplApp(lambda, help)
  }
}
