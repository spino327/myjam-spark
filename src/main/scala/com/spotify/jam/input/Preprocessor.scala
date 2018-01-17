
package com.spotify.jam.input

import scala.collection.mutable._
import scala.util.matching.Regex

/**
 * Utility singleton to parse the input data
 */
object Preprocessor {

  /**
   * Parse the info of a jam by:
   * - filters out Punctuation symbols: !"#$%&'()*+,-./:;<=>?@[\]^_`{|}~
   *
   * @return Array with the jam's values for the first 4 columns: jam_id, user_id, artist, title  
   */
  def parseJam (line: String) : Array[String] = {
    return parse(line, raw"\p{Punct}".r, 4) 
  }

  /**
   * Parse the info of a like by:
   * 
   * @return Array with the likes' values for the columns  
   */
  def parseLike (line: String) : Array[String] = {
    return parse(line, null, 2) 
  }
  
  /**
   * Returns an array of string by parsing a line:
   * - split it by tabs
   * - removes characters using the @param filterOut regex
   * - ignores extra tokens in cases where the line has more tokens than @param size
   */
  private def parse (line: String, filterOut:Regex, size: Int) : Array[String] = {
    
    val res = new ArrayBuffer[String]()
   
    var filteredLine:String = null
    if (filterOut != null)
      filteredLine = filterOut.replaceAllIn(line, " ")
    else
      filteredLine = line
    
    var left = 0
    var right = line.indexOf('\t')
    var pos = 0
    while (left <= right && pos < size ) {
      res.append(filteredLine.substring(left, right))
      left = right + 1
      right = filteredLine.indexOf('\t', left)
      pos = pos + 1
    }
    if (pos < size) {
      if (left < filteredLine.length) 
        res.append(filteredLine.substring(left))
      else
        res.append("")
    }

    return res.toArray
  }
}
