
package com.spotify.jam.input

import org.junit.Test
import org.junit.Assert._

class PreprocessorTest {

  /*
   * Test for Jam
   */
  @Test
  def testParseCompleteJam = {

    // jam_id, user_id, artist, title, creadtion_date, link, spotify_uri
    val line = "c2e76bb92c7fa733fdfc9be40bb0e4ea\tb99ebf68a8d93f024e56c65e2f949b57\tOrange-Juice\tRip It Up\t2011-08-26\thttp://some.com\tspotify:track:6AGhDIyDbRonzGTdbIsNXa"
    val exp_split = Array("c2e76bb92c7fa733fdfc9be40bb0e4ea", "b99ebf68a8d93f024e56c65e2f949b57", "Orange Juice", "Rip It Up")
    val res_split = Preprocessor.parseJam(line)
    
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
  }

  @Test
  def testParsePartialJam = {

    // jam_id, user_id, artist, title, creadtion_date, link, spotify_uri
    val line = "c2e76bb92c7fa733fdfc9be40bb0e4ea\tb99ebf68a8d93f024e56c65e2f949b57\tOrange-Juice\t\t2011-08-26\t\tspotify:track:6AGhDIyDbRonzGTdbIsNXa"
    val exp_split = Array("c2e76bb92c7fa733fdfc9be40bb0e4ea", "b99ebf68a8d93f024e56c65e2f949b57", "Orange Juice", "")
    val res_split = Preprocessor.parseJam(line)

    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
  }

  @Test
  def testParseIllJam = {

    // jam_id, user_id, artist, title, creadtion_date, link, spotify_uri
    var line = "c2e76bb92c7fa733fdfc9be40bb0e4ea\tb99ebf68a8d93f024e56c65e2f949b57"
    var exp_split = Array("c2e76bb92c7fa733fdfc9be40bb0e4ea", "b99ebf68a8d93f024e56c65e2f949b57")
    var res_split = Preprocessor.parseJam(line)
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
    
    line = "c2e76bb92c7fa733fdfc9be40bb0e4ea\tb99ebf68a8d93f024e56c65e2f949b57\t0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t"
    exp_split = Array("c2e76bb92c7fa733fdfc9be40bb0e4ea", "b99ebf68a8d93f024e56c65e2f949b57", "0", "1")
    res_split = Preprocessor.parseJam(line)
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
    
    line = "\t\t\t\t\t\t\t\t\t\t\t\t\t"
    exp_split = Array("", "", "", "")
    res_split = Preprocessor.parseJam(line)
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
  }

  /*
   * Tests for processing likes
   */
  @Test
  def testParseCompleteLike = {

    // user_id, jam_id
    val line = "c1066039fa61eede113878259c1222d1\t5d2bc46196d7903a5580f0dbedc09610"
    val exp_split = Array("c1066039fa61eede113878259c1222d1", "5d2bc46196d7903a5580f0dbedc09610")
    val res_split = Preprocessor.parseLike(line)
    
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
  }

  @Test
  def testParsePartialLike = {

    val line = "c1066039fa61eede113878259c1222d1\t"
    val exp_split = Array("c1066039fa61eede113878259c1222d1", "")
    val res_split = Preprocessor.parseLike(line)

    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])    
  }

  @Test
  def testParseIllLike = {

    var line = "c1066039fa61eede113878259c1222d1"
    var exp_split = Array("c1066039fa61eede113878259c1222d1")
    var res_split = Preprocessor.parseLike(line)
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
    
    line = "c1066039fa61eede113878259c1222d1\t5d2bc46196d7903a5580f0dbedc09610\t0\t1\t2\t3\t4\t5\t6\t7\t8\t9\t10\t"
    exp_split = Array("c1066039fa61eede113878259c1222d1", "5d2bc46196d7903a5580f0dbedc09610")
    res_split = Preprocessor.parseLike(line)
    assertArrayEquals(exp_split.asInstanceOf[Array[Object]], res_split.asInstanceOf[Array[Object]])
  }
}
