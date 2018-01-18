
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
      ).cache() // to avoid recomputation of userTrackVector during the cartesian product
      println(userTrackVector.take(1))
      
      /**
       * Computing the unique users
       */
      val users = userTrackVector.keys.cache() // to avoid re-computation
      println(userTrackVector.take(1))
      
      /*
       * Computing cosine similarity of the 'track vectors' of each pair of <user1,user2>
       * The most similar pair of <user1,user2> will be those with the highest cosine similarity value
       * 
       * - We use cartesian product to generate all the permutations of users. Thus, this is inneficient 'cuz
       * - The filter applied after the cartesian product filters out the bottom half of the matrix
       * - we compute the similarity of (user1, user2) and (user2, user1). 
       * 
       * cos_sim(u1,u2) = dot_product(v1, v2)    
       */      
      val lambdaCosineSim = (vec1:HashMap[Tuple2[String, String], Int], vec2:HashMap[Tuple2[String, String], Int]) => {
        // getting the common keys
        val keys = HashSet[Tuple2[String, String]]()
        keys ++ vec1.keySet
        keys ++ vec2.keySet
        
        // computing dot product
        var sim = 0
        for (key <- keys) {
          // multiply #reproduction of art_tit for both users
          val e1:Int = if (vec1.contains(key)) vec1(key) else 0
          val e2:Int = if (vec2.contains(key)) vec2(key) else 0
          
          sim = sim + e1 * e2
        }
//        val dotProduct = keys.map(art_tit => {
//          // multiply #reproduction of art_tit for both users
//          val e1:Int = { 
//            if (vec1.contains(art_tit)) vec1(art_tit) else 0
//          }
//          val e2:Int = { 
//            if (vec2.contains(art_tit)) vec2(art_tit) else 0
//          }
//          e1 * e2
//        }).reduce((prod1, prod2) => prod1 + prod2) // sum
        sim
      }
      
      // computing the cartesian product between users
      // this is very costly
      val usersPair = users.cartesian(users) // cartesian product
        .filter({
          // filters out the bottom half of the matrix
          case (uid1, uid2) => {
            // user_id1 < user_id2
            if (uid1 < uid2)
              true
            else
              false
          }
        })
      
      val topN = usersPair.join(userTrackVector) // (u1, (u2, v1))
        .map({ // flips user u2 as key
          case (k, v) => (v._1, (k, v._2)) // (u2, (u1, v1))
        }) 
        .join(userTrackVector) // (u2, ((u1, v1), v2)) Now we got all the 2 user vectors
        .map({ // computes the cosine similarity between peair of users
          case (u2, value) => {
            val u1 = value._1._1
            val v1 = value._1._2
            val v2 = value._2
            // pairRDD <sim, (user_id1, user_id2)>
            (lambdaCosineSim(v1, v2), // computes cosine similarity and uses it as key 
              (u1, u2))
          }      
        })
        .top(NUM_USERS) // pick top N
        .map({ case (v, k) => s"$k, $v"}).mkString("\n") // creates string of results
           
//        .map({ case (v, k) => s"$k, $v"}).mkString("\n") // creates string of results
//      val topN = userTrackVector.cartesian(userTrackVector) // cartesian product
//        .filter({ // filters out the bottom half of the matrix
//          case (user_v1, user_v2) => {
//            val uid1 = user_v1._1
//            val uid2 = user_v2._1
//            // user_id1 < user_id2
//            if (uid1 < uid2)
//              true
//            else
//              false
//          }
//        }).saveAsTextFile(output + "/filter")
//        .map({ // computes the cosine similarity between peair of users
//          case (user_v1, user_v2) => {
//            // pairRDD <sim, (user_id1, user_id2)>
//            (lambdaCosineSim(user_v1._2, user_v2._2), // computes cosine similarity and uses it as key 
//              (user_v1._1, user_v2._1))
//          }    
//        })
//        .top(NUM_USERS) // pick top N
//        .map({ case (v, k) => s"$k, $v"}).mkString("\n") // creates string of results
              
      // create the output file
      val pw = new PrintWriter(new File(output + "/topSimUsers.csv"))
      pw.write("(user_id1 : user_id2), Cosine similarity\n")
      pw.write(topN)
      pw.close()
      printMsg("(user_id1 : user_id2), Cosine similarity\n" + topN)
      
    }

    // Help comments
    val help = "What were the top N most similar users of This is My Jam?" + 
      "\ttop_sim <N (optional)>. Where N is 5 by default."

    return new ReplApp(lambda, help)
  }
}
