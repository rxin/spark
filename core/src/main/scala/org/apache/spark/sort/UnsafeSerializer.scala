package org.apache.spark.sort

import java.io.{OutputStream, InputStream}
import java.nio.ByteBuffer

import scala.reflect.ClassTag

import org.apache.spark.serializer._


final class UnsafeSerializer(recordsPerPartition: Long) extends Serializer with Serializable {

  private[this] var offset = 0L

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    offset += recordsPerPartition
    new UnsafeSerializerInstance(offset)
  }
}


final class UnsafeSerializerInstance(offset: Long) extends SerializerInstance {
  override def serializeStream(s: OutputStream): SerializationStream = {
    new UnsafeSerializationStream(s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new UnsafeDeserializationStream(s, offset)
  }

  override def serialize[T: ClassTag](t: T): ByteBuffer = ???

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = ???

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = ???
}


final class UnsafeSerializationStream(s: OutputStream) extends SerializationStream {

  private[this] val buf = new Array[Byte](100)

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    // The record is expected to be (Long, Long), where _1 is the address of the record
    // Copy 100 bytes from the off-heap memory into buf, and write that out.
    val addr: Long = t.asInstanceOf[(Long, Long)]._1
    UnsafeSort.UNSAFE.copyMemory(null, addr, buf, UnsafeSort.BYTE_ARRAY_BASE_OFFSET, 100)
    s.write(buf)
    this
  }

  override def flush(): Unit = s.flush()

  override def close(): Unit = s.close()
}


final class UnsafeDeserializationStream(s: InputStream, offset0: Long)
  extends DeserializationStream {

  private[this] val buf = new Array[Byte](100)
  private[this] val blockAddress = UnsafeSort.blocks.get().toLong
  private[this] var offset: Long = blockAddress + offset0

  override def readObject[T: ClassTag](): T = {
    // Read 100 bytes into the buffer, and then copy that into the off-heap block.
    // Return the address of the 100-byte in the off heap block.
    s.read(buf)
    UnsafeSort.UNSAFE.copyMemory(
      buf, UnsafeSort.BYTE_ARRAY_BASE_OFFSET, null, offset, 100)
    offset += 100
    (offset, 0L).asInstanceOf[T]
  }

  override def close(): Unit = s.close()
}
