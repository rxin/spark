package org.apache.spark.network.netty

import java.io.{FileInputStream, File}
import java.nio.channels.{FileChannel, WritableByteChannel}

import io.netty.channel.FileRegion
import io.netty.util.AbstractReferenceCounted


final class LazyFileRegion(val file: File, override val position: Long, override val count: Long)
  extends AbstractReferenceCounted with FileRegion {

  private[this] var channel: FileChannel = null

  private[this] var _transfered: Long = 0L

  override def deallocate(): Unit = {
    if (channel != null) {
      channel.close()
    }
  }

  override def transferTo(target: WritableByteChannel, position: Long): Long = {
    if (channel == null) {
      channel = new FileInputStream(file).getChannel
    }

    val count = this.count - position
    if (count < 0 || position < 0) {
      throw new IllegalArgumentException(
        "position out of range: " + position + " (expected: 0 - " + (count - 1) + ')')
    }

    if (count == 0) {
      return 0L
    }

    val written: Long = channel.transferTo(this.position + position, count, target)
    if (written > 0) {
      _transfered += written
    }
    written
  }

  override def transfered(): Long = _transfered
}
