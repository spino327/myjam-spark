package com.spotify.jam

import com.spotify.jam.input._
import com.spotify.jam.apps._

import java.io._

import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.sys
import scala.io.StdIn
//import Console.{GREEN, RED, RESET, YELLOW_B, UNDERLINED}

/*
 *
 */
object MyJamApp {

  // repl variable
  private val repl = new Repl()

  /**
   * Installing REPL applications
   */
  def installingApps(output:String, sc:SparkContext):Unit = {    
    // adding basic
    repl.installOption("basic" -> BasicApp.makeApp(output, sc))
    // adding top jams
    repl.installOption("top_jams" -> TopJamsApp.makeApp(output, sc))
    // adding top artists
    repl.installOption("top_arts" -> TopArtistsApp.makeApp(output, sc))
    // adding top similar users
    repl.installOption("top_sim" -> TopSimilarUsersApp.makeApp(output, sc))
  }

  def main (args: Array[String]) {

    if (args.length < 5) {
      println("USAGE: MyJamApp <jams> <likes> <followers> <output_folder> <num_partitions>")
      sys.exit(-1)
    }

    // Input/Output files
    val inputJams = args(0)
    val inputLikes = args(1)
    val inputFollowers = args(2)
    val outputFolder = args(3)
    val numPartitions = args(4).toInt

    // create folder
    new File(outputFolder).mkdir()
    
    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("ThisIsMyJam Spark") 
    val sc = new SparkContext(conf)
    
    // installing apps
    installingApps(outputFolder, sc)
    
    // starting repl
    repl.loop(inputJams, inputLikes, inputFollowers)
  }
}

