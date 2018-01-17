
package com.spotify.jam.apps

import com.spotify.jam.input._
import com.spotify.jam.apps.ReplHelper._

import java.io._

import scala.collection.mutable._

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

/**
 * What were the top N most similar users of This is My Jam. 
 * Please suggest and explain your choice of similarity measure. Please provide user_ids.
 * 
 * Here we use the cosine similarity between two users.
 * The representation for each user is based on a vector space model (bag of words). Each user
 * is a vector where each element is the number of likes the user has give to each song (artist, title)
 * in the dataset.
 */
object TopSimilarUsersApp {

  /** Returns the ReplApp for analyzing the top N similar users*/
  def makeApp (output:String, sc:SparkContext):ReplApp = {

    // Lambda for processing
    val lambda = (fileJams:String, fileLikes:String, fileFollowers:String, args:Array[String]) => {

      val NUM_USERS = { 
        if (args.length > 0)
          args(0).toInt
        else
          5
      }
      
      printMsg(s"Computing Top$NUM_USERS similar users" )
      
      /*
       * VECTOR SPACE MODEL:
       * Computing track vector for each user_id <user_id, Map of tracks> from Jams file
       * Track vector is a Sparse vector (Map) with the number of times a particular track was a jam for the user.
       * track = <artist, title>
       */
      // Load jams data
      val rawJams =  sc.textFile(fileJams)
      
      // creating pairRDD <user_id, <artist, title>>
      val userTrack = rawJams.map(line => {

        // parse jam
        // lower case to clean up data
        // Assuming user_id is case-insensitive
        val jamData:Array[String] = Preprocessor.parseJam(line.toLowerCase())

        // we required at least jam_id, user_id, artist, title
        if (jamData.length >= 4)
          // <user_id, (artist, title)>
          (jamData(1) -> (jamData(2) -> jamData(3)))
        else
          ("" -> (""->""))
      })
      
      // creating pairRDD <user_id, Map of tracks>
      val lambdaNewCombiner = (artist_title:Tuple2[String, String]) => { 
        HashMap[Tuple2[String, String], Int] ((artist_title -> 1))
      }
      
      val lambdaMerger = (vec:HashMap[Tuple2[String, String], Int], artist_title:Tuple2[String, String]) => {
        val current:Int = {
          if (vec.contains(artist_title))
            vec(artist_title)
          else
            0
        }
        vec += (artist_title -> (current + 1))
      }
      
      val lambdaMergeCombiners = (vec1: HashMap[Tuple2[String, String], Int], vec2: HashMap[Tuple2[String, String], Int]) => {
        val newVect = HashMap[Tuple2[String, String], Int] ()
        
        // adds the KV from vec1 and vec2
        val vectors = vec1 :: vec2 :: Nil
        
        vectors.foreach( vector => {
          vector.foreach({ 
            case (k, v) => {
              val current:Int = {
            		  if (newVect.contains(k))
            			  newVect(k)
            			else
            			  0
              }
              newVect += (k -> (current + v)) 
            }
          })
        })
        
        newVect
      }
      
      // This PairRDD has the <user_id, vector of tracks>
      // a.k.a vector space model for the tracks played by the user
      val userTrackVector = userTrack.combineByKey(
          lambdaNewCombiner, // New Combiner
          lambdaMerger, // Merger
          lambdaMergeCombiners // Combiner
      )
      
      /*
       * Computing cosine similarity of the 'track vectors' of each pair of <user1,user2>
       * The most similar pair of <user1,user2> will be those with the highest cosine similarity value
       * 
       * We use cartesian product to generate all the permutations of users. Thus, this is inneficient 'cuz
       * The map applied after the cartesian product filterswe compute the similarity of (user1, user2) and (user2, user1). 
       * 
       * cos_sim(u1,u2) = dot_product(v1, v2)    
       */
      val completeCount = userTrackVector.cartesian(userTrackVector).count()
      
      val halfMatrix = userTrackVector.cartesian(userTrackVector) // cartesian product
        .map({
          case (user_v1, user_v2) => {
            val user1 = user_v1._1
            val user2 = user_v2._1
            if (user1 < user2)
              // ((user1, user2), (vector1, vector2))
              ((user1, user2) -> (user_v1._2, user_v2._2))
            else
              (("","") -> null)
          }
        }) // this map 
        .count()
        
      printErr(s"completeCount: $completeCount, halfMatrix: $halfMatrix")      
      
//      /*
//       * Joining jamArtist + jamLikesCount to get <jam_id, artist, #likes>
//       * We do it by performing a natural join since we don't care for those
//       * jams that don't have likes
//       */
//      // PairRDD[jam_id, [artist, #likes]]
//      val jamArtistLikes = jamArtist.join(jamLikesCount)
//      
//      /*
//       * Computing top <artist, #likes>
//       */
//      // performing likes count per artist <artist, #likes> from PairRDD[jam_id, [artist, #likes]]
//      val artistLikes = jamArtistLikes.map({ 
//        // creating RDD <artist, #likes>
//        case (jam_id, artist_likes) => (artist_likes._1 -> artist_likes._2)
//      }).reduceByKey((v1, v2) => v1 + v2) 
//      
//      val topN = artistLikes.map({ case (artist, n_likes) => (n_likes, artist)}) // flips
//        .top(NUM_ART) // pick top N
//        .map({ case (v, k) => s"$k, $v"}).mkString("\n") // creates string of results
//        
//      // create the output file
//      val pw = new PrintWriter(new File(output + "/topArtists.csv"))
//      pw.write("Artist, #Likes\n")
//      pw.write(topN)
//      pw.close()
//      printMsg("Artist, #Likes\n" + topN)
      
    }

    // Help comments
    val help = "What were the top N most similar users of This is My Jam?" + 
      "\ttop_sim <N (optional)>. Where N is 5 by default."

    return new ReplApp(lambda, help)
  }
}
