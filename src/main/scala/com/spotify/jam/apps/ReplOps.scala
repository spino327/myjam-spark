
package com.spotify.jam.apps

import Console.{GREEN, RED, RESET, YELLOW_B, UNDERLINED}

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._

import scala.io.StdIn

/**
 * Read-eval-print loop
 */
class Repl {
  /** Map of REPL applications*/
  private val processMap = scala.collection.mutable.HashMap[String, ReplApp]()
  /** Lambda for printing normal message */
  val printMsg = (x:String) => Console.println(s"${RESET}${GREEN}$x${RESET}")
  /** Lambda for printing prompt*/
  val printPrompt = () => Console.print(s"${RESET}${GREEN}${UNDERLINED} > ${RESET}")
  /** Lambda for printing errors*/
  val printErr = (x:String) => Console.println(s"${RESET}${RED}$x${RESET}") 

  /** Adds a REPL applications*/
  def installOption(option: Tuple2[String, ReplApp]): Unit = {
    processMap += option
  }

  /** Start REPL loop. */
  def loop(fileJams:String, fileLikes:String, fileFollowers:String): Unit = {
    printMsg("Welcome to the REPL. Typed 'help' for options")

    var poisonPill = false
    while (poisonPill != true) {
      printPrompt()
      val input_split = StdIn.readLine().split("\\s")
      val option = input_split(0)
      
      option match {
        case "exit" => { 
          printErr("Stopping REPL...")
          poisonPill = true
        }
        case _ => {
          if (processMap.contains(option)) {
            val init = System.nanoTime()
            processMap(option).process(fileJams, fileLikes, fileFollowers, input_split.tail)
            ReplHelper.printErr("Execution time [sec]: " + ((System.nanoTime() - init)/1.0e9))
          } else {
            Console.println(s"${RESET}${GREEN}help : ${RESET}shows this help")
            processMap.foreach(x => Console.println(s"${RESET}${GREEN}" + x._1 + s" : ${RESET}" + x._2.help()))
            Console.println(s"${RESET}${GREEN}exit : ${RESET}exits the REPL")
          }
        }
      }
    }
  }
}

/** Base class for REPL applications*/
class ReplApp(lambda:Function4[String, String, String, Array[String], Unit], helpStr: String) {

  /** Return help for this app*/
  def help () : String = {
    return helpStr
  }

  /** Process the input*/
  def process (fileJams:String, fileLikes:String, fileFollowers:String, args:Array[String]) : Unit = {
    lambda(fileJams, fileLikes, fileFollowers, args)
  } 
}

/** Helper singleton*/
object ReplHelper {
  def printMsg(x:String) = Console.println(s"${RESET}${GREEN}$x${RESET}")
  def printErr(x:String) = Console.println(s"${RESET}${RED}$x${RESET}")
}
