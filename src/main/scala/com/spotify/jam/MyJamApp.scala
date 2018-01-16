package com.spotify.jam

import com.spotify.jam.input._
import com.spotify.jam.apps._

import java.io.File

import org.apache.spark._
import org.apache.spark.SparkContext._

import scala.sys
import scala.io.StdIn
import Console.{GREEN, RED, RESET, YELLOW_B, UNDERLINED}

/*
 *
 */
object MyJamApp {

  // repl variable
  private val repl = new Repl()

  /**
   * Installing REPL applications
   */
  def installingApps(output:String):Unit = {
    // // adding saving songs data app
    // repl.installOption("save_data" -> new ReplApp((x) => {
    //   println("Saving songs data...")
    //   x.saveAsTextFile(output + "/songs_data")
    // }, "Saving songs data."))
    
    // adding basic
    repl.installOption("basic" -> BasicApp.makeApp(output))

    // adding top jams
    repl.installOption("top_jams" -> TopJamsApp.makeApp(output))
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
  
    // installing apps
    installingApps(outputFolder)

    // Create a Scala Spark Context.
    val conf = new SparkConf().setAppName("ThisIsMyJam Spark") 
    val sc = new SparkContext(conf)
    
    // Load jams data
    val rawJams =  sc.textFile(inputJams, numPartitions)

    val totalLines = rawJams.count()

    val jamsMap = rawJams.map(line => {
        val jamData:Array[String] = Preprocessor.parseJam(line)
        
        // we required at least jam_id, user_id, artist, title
        if (jamData.length >= 4)
          ((jamData(2) -> jamData(3)), 1)
        else
          (("" -> ""), 1)
    })

    val jamsCount = jamsMap.reduceByKey((v1, v2) => v1 + v2)
    var init = System.nanoTime()
    val top5_1 = jamsCount.map({ case (k, v) => (v, k)}).groupByKey().sortByKey().top(15)
    println(top5_1.mkString("\n"))
    val ex1 = System.nanoTime() - init

    init = System.nanoTime()
    val top5_2 = jamsCount.map({ case (k, v) => (v, k)}).groupByKey().top(15)
    println(top5_2.mkString("\n"))
    val ex2 = System.nanoTime() - init
    println("execution time ex1: " + ex1/1.0e9 + ", exp2: " + ex2/1.0e9)
    // top5.saveAsTextFile(outputFolder)
    println(s"totalLines: $totalLines")
    // println(s"totalLines: $totalLines, min_split: " + sizeSplit.min() + ", max_split: " + sizeSplit.max())
    

    // starting repl
    // repl.loop(pairSongDataRDD)

  }
}

