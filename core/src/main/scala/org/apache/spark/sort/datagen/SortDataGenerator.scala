package org.apache.spark.sort.datagen

import java.io.{FileOutputStream, BufferedOutputStream, File}
import java.util.concurrent.atomic.AtomicInteger

import org.apache.hadoop.io.nativeio.NativeIO
import org.apache.spark.sort.old.Sort
import org.apache.spark.sort.{NodeLocalRDDPartition, NodeLocalRDD}
import org.apache.spark.{TaskContext, Partition, SparkConf, SparkContext}


object SortDataGenerator {

  private[this] val numTasksOnExecutor = new AtomicInteger

  def main(args: Array[String]): Unit = {
    val sizeInGB = args(0).toInt
    val numParts = args(1).toInt
    val dirs = args(2).split(",").map(_ + s"/sort-${sizeInGB}g-$numParts").toSeq

    val sc = new SparkContext(new SparkConf().setAppName(
      s"DataGeneratorJava - $sizeInGB GB - $numParts parts - ${args(2)}"))

    genSort(sc, sizeInGB, numParts, dirs)
  }

  def genSort(sc: SparkContext, sizeInGB: Int, numParts: Int, dirs: Seq[String]): Unit = {
    val sizeInBytes = sizeInGB.toLong * 1000 * 1000 * 1000
    val numRecords = sizeInBytes / 100
    val recordsPerPartition = math.ceil(numRecords.toDouble / numParts).toLong

    val hosts = Sort.readSlaves()

    val output = new NodeLocalRDD[(String, Int, String)](sc, numParts, hosts) {
      override def compute(split: Partition, context: TaskContext) = {
        val part = split.index
        val host = split.asInstanceOf[NodeLocalRDDPartition].node
        val baseFolder = dirs(numTasksOnExecutor.getAndIncrement() % dirs.size)

        val iter = generatePartition(part, recordsPerPartition.toInt)

        //val baseFolder = s"/vol$volIndex/sort-${sizeInGB}g-$numParts"
        if (!new File(baseFolder).exists()) {
          new File(baseFolder).mkdirs()
        }
        val outputFile = s"$baseFolder/part$part.dat"

        val fileOutputStream = new FileOutputStream(outputFile)
        val os = new BufferedOutputStream(fileOutputStream, 4 * 1024 * 1024)
        while (iter.hasNext) {
          val buf = iter.next()
          os.write(buf)
        }

        NativeIO.POSIX.getCacheManipulator.posixFadviseIfPossible(
          outputFile,
          fileOutputStream.getFD,
          0,
          recordsPerPartition * 100,
          NativeIO.POSIX.POSIX_FADV_DONTNEED)

        {
          val startTime = System.currentTimeMillis()
          NativeIO.POSIX.syncFileRangeIfPossible(
            fileOutputStream.getFD,
            0,
            recordsPerPartition * 100,
            NativeIO.POSIX.SYNC_FILE_RANGE_WRITE)
          val timeTaken = System.currentTimeMillis() - startTime
          logInfo(s"fsync $outputFile took $timeTaken ms")
        }

        os.close()

        Iterator((host, part, outputFile))
      }
    }.collect()

    output.foreach { case (host, part, outputFile) =>
      println(s"$part\t$host\t$outputFile")
    }

    val checksumFile = s"/root/sort-${sizeInGB}g-$numParts.output"
    println(s"checksum output: $checksumFile")
    val writer = new java.io.PrintWriter(new File(checksumFile))
    output.foreach {  case (host, part, outputFile) =>
      writer.write(s"$part\t$host\t$outputFile\n")
    }
    writer.close()
  }  // end of genSort

  def generatePartition(part: Int, recordsPerPartition: Int): Iterator[Array[Byte]] = {
    val one = new Unsigned16(1)
    val firstRecordNumber = new Unsigned16(part.toLong * recordsPerPartition)

    new Iterator[Array[Byte]] {
      private[this] val buf = new Array[Byte](100)
      private[this] val rand = Random16.skipAhead(firstRecordNumber)
      private[this] val recordNumber = new Unsigned16(firstRecordNumber)

      private[this] var i = 0
      private[this] val recordsPerPartition0 = recordsPerPartition

      override def hasNext: Boolean = i < recordsPerPartition0
      override def next(): Array[Byte] = {
        Random16.nextRand(rand)
        generateRecord(buf, rand, recordNumber)
        recordNumber.add(one)
        i += 1
        buf
      }
    }
  }

  /**
   * Generate a binary record suitable for all sort benchmarks except PennySort.
   *
   * @param recBuf record to return
   */
  private def generateRecord(recBuf: Array[Byte], rand: Unsigned16, recordNumber: Unsigned16): Unit = {
    // Generate the 10-byte key using the high 10 bytes of the 128-bit random number
    var i = 0
    while (i < 10) {
      recBuf(i) = rand.getByte(i)
      i += 1
    }

    // Add 2 bytes of "break"
    recBuf(10) = 0x00.toByte
    recBuf(11) = 0x11.toByte

    // Convert the 128-bit record number to 32 bits of ascii hexadecimal
    // as the next 32 bytes of the record.
    i = 0
    while (i < 32) {
      recBuf(12 + i) = recordNumber.getHexDigit(i).toByte
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(44) = 0x88.toByte
    recBuf(45) = 0x99.toByte
    recBuf(46) = 0xAA.toByte
    recBuf(47) = 0xBB.toByte

    // Add 48 bytes of filler based on low 48 bits of random number
    i = 0
    while (i < 12) {
      val v = rand.getHexDigit(20 + i).toByte
      recBuf(48 + i * 4) = v
      recBuf(49 + i * 4) = v
      recBuf(50 + i * 4) = v
      recBuf(51 + i * 4) = v
      i += 1
    }

    // Add 4 bytes of "break" data
    recBuf(96) = 0xCC.toByte
    recBuf(97) = 0xDD.toByte
    recBuf(98) = 0xEE.toByte
    recBuf(99) = 0xFF.toByte
  }

}
