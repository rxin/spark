package org.apache.spark.sort

import java.io._


object Utils {

  def readSlaves(): Array[String] = {
    scala.io.Source.fromFile("/root/hosts.txt").getLines().toArray
  }

  /** Run a command, and return exit code, stdout, and stderr. */
  def runCommand(cmd: String): (Int, String, String) = {
    println("running system command: " + cmd)
    val pb = new java.lang.ProcessBuilder(cmd.split(" ") : _*)
    val p = pb.start()
    val exitCode = p.waitFor()

    def read(is: InputStream): String = {
      val buf = new StringBuffer
      val b = new BufferedReader(new InputStreamReader(is))
      var line = b.readLine()
      while (line != null) {
        buf.append(line + "\n")
        line = b.readLine()
      }
      b.close()
      buf.toString
    }

    val stdout = read(p.getInputStream)
    val stderr = read(p.getErrorStream)
    println(s"=====================\nstdout for $cmd:\n$stdout\n==========================")
    println(s"=====================\nstderr for $cmd:\n$stderr\n==========================")
    (exitCode, stdout, stderr)
  }

}
