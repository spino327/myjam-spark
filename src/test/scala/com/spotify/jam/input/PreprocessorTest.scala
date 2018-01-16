
package com.spotify.jam.input

import org.junit.Test
import org.junit.Assert._

class PreprocessorTest {

  @Test
  def testParseCompleteJam = {
    println("testParseCompleteJam")

    // jam_id, user_id, artist, title, creadtion_date, link, spotify_uri
    val line = "c2e76bb92c7fa733fdfc9be40bb0e4ea\tb99ebf68a8d93f024e56c65e2f949b57\tOrange Juice\tRip It Up\t2011-08-26\thttp://some.com\tspotify:track:6AGhDIyDbRonzGTdbIsNXa"
    val exp_split = Array("c2e76bb92c7fa733fdfc9be40bb0e4ea", "b99ebf68a8d93f024e56c65e2f949b57", "orange juice", "rip it up", "2011-08-26", "http://some.com", "spotify:track:6aghdiydbronzgtdbisnxa")
    val res_split = Preprocessor.parseJam(line)
    
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
  }

  @Test
  def testParsePartialJam = {
    println("testParsePartialJam")

    // jam_id, user_id, artist, title, creadtion_date, link, spotify_uri
    val line = "c2e76bb92c7fa733fdfc9be40bb0e4ea\tb99ebf68a8d93f024e56c65e2f949b57\tOrange Juice\t\t2011-08-26\t\tspotify:track:6AGhDIyDbRonzGTdbIsNXa"
    val exp_split = Array("c2e76bb92c7fa733fdfc9be40bb0e4ea", "b99ebf68a8d93f024e56c65e2f949b57", "orange juice", "", "2011-08-26", "", "spotify:track:6aghdiydbronzgtdbisnxa")
    val res_split = Preprocessor.parseJam(line)

    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
  }

  @Test
  def testParseIllJam = {
    println("testParseIllJam")

    // jam_id, user_id, artist, title, creadtion_date, link, spotify_uri
    var line = "c2e76bb92c7fa733fdfc9be40bb0e4ea\tb99ebf68a8d93f024e56c65e2f949b57"
    var exp_split = Array("c2e76bb92c7fa733fdfc9be40bb0e4ea", "b99ebf68a8d93f024e56c65e2f949b57")
    var res_split = Preprocessor.parseJam(line)
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
    
    line = "c2e76bb92c7fa733fdfc9be40bb0e4ea\tb99ebf68a8d93f024e56c65e2f949b57\t0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t"
    exp_split = Array("c2e76bb92c7fa733fdfc9be40bb0e4ea", "b99ebf68a8d93f024e56c65e2f949b57", "0", "1", "2", "3", "4\t5\t6\t7\t8\t9\t10\t")
    res_split = Preprocessor.parseJam(line)
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
  }

  /*
   * Tests for processing likes
   */
  @Test
  def testParseCompleteLike = {
    println("testParseCompleteLike")

    // user_id, jam_id
    val line = "c1066039fa61eede113878259c1222d1\t5d2bc46196d7903a5580f0dbedc09610"
    val exp_split = Array("c1066039fa61eede113878259c1222d1", "5d2bc46196d7903a5580f0dbedc09610")
    val res_split = Preprocessor.parseLike(line)
    
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
  }

  @Test
  def testParsePartialJam = {
    println("testParsePartialJam")

    // jam_id, user_id, artist, title, creadtion_date, link, spotify_uri
    val line = "c2e76bb92c7fa733fdfc9be40bb0e4ea\tb99ebf68a8d93f024e56c65e2f949b57\tOrange Juice\t\t2011-08-26\t\tspotify:track:6AGhDIyDbRonzGTdbIsNXa"
    val exp_split = Array("c2e76bb92c7fa733fdfc9be40bb0e4ea", "b99ebf68a8d93f024e56c65e2f949b57", "orange juice", "", "2011-08-26", "", "spotify:track:6aghdiydbronzgtdbisnxa")
    val res_split = Preprocessor.parseJam(line)

    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
  }

  @Test
  def testParseIllJam = {
    println("testParseIllJam")

    // jam_id, user_id, artist, title, creadtion_date, link, spotify_uri
    var line = "c2e76bb92c7fa733fdfc9be40bb0e4ea\tb99ebf68a8d93f024e56c65e2f949b57"
    var exp_split = Array("c2e76bb92c7fa733fdfc9be40bb0e4ea", "b99ebf68a8d93f024e56c65e2f949b57")
    var res_split = Preprocessor.parseJam(line)
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
    
    line = "c2e76bb92c7fa733fdfc9be40bb0e4ea\tb99ebf68a8d93f024e56c65e2f949b57\t0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t"
    exp_split = Array("c2e76bb92c7fa733fdfc9be40bb0e4ea", "b99ebf68a8d93f024e56c65e2f949b57", "0", "1", "2", "3", "4\t5\t6\t7\t8\t9\t10\t")
    res_split = Preprocessor.parseJam(line)
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
  }
}
