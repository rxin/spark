package org.apache.spark.sort

import java.io.{EOFException, OutputStream, InputStream}
import java.nio.ByteBuffer

import org.apache.spark.util.MutablePair

import scala.reflect.ClassTag

import org.apache.spark.serializer._


final class UnsafeSerializer(recordsPerPartition: Long) extends Serializer with Serializable {

  var offset = 0L

  /** Creates a new [[SerializerInstance]]. */
  override def newInstance(): SerializerInstance = {
    val inst = new UnsafeSerializerInstance(this)
    inst
  }
}


final class UnsafeSerializerInstance(ser: UnsafeSerializer) extends SerializerInstance {
  override def serializeStream(s: OutputStream): SerializationStream = {
    new UnsafeSerializationStream(s)
  }

  override def deserializeStream(s: InputStream): DeserializationStream = {
    new UnsafeDeserializationStream(s, ser)
  }

  override def serialize[T: ClassTag](t: T): ByteBuffer = ???

  override def deserialize[T: ClassTag](bytes: ByteBuffer): T = ???

  override def deserialize[T: ClassTag](bytes: ByteBuffer, loader: ClassLoader): T = ???
}


final class UnsafeSerializationStream(s: OutputStream) extends SerializationStream {

  private[this] val buf = new Array[Byte](100)
  private[this] val BYTE_ARRAY_BASE_OFFSET = UnsafeSort.BYTE_ARRAY_BASE_OFFSET

  override def writeObject[T: ClassTag](t: T): SerializationStream = {
    // The record is expected to be (Long, Long), where _1 is the address of the record
    // Copy 100 bytes from the off-heap memory into buf, and write that out.
    val addr: Long = t.asInstanceOf[MutablePair[Long, Long]]._1
    UnsafeSort.UNSAFE.copyMemory(null, addr, buf, BYTE_ARRAY_BASE_OFFSET, 100)
    s.write(buf)
    this
  }

  override def flush(): Unit = s.flush()

  override def close(): Unit = s.close()
}


final class UnsafeDeserializationStream(s: InputStream, ser: UnsafeSerializer)
  extends DeserializationStream {

  private[this] val buf = new Array[Byte](100)
  private[this] val BYTE_ARRAY_BASE_OFFSET: Long = UnsafeSort.BYTE_ARRAY_BASE_OFFSET
  private[this] val sortBuffer = UnsafeSort.sortBuffers.get()

  override def readObject[T: ClassTag](): T = {
    // Read 100 bytes into the buffer, and then copy that into the off-heap block.
    // Return the address of the 100-byte in the off heap block.
    val read = s.read(buf)
    if (read < 0) {
      throw new EOFException
    }
    assert(read == 100)
    val addr = sortBuffer.address + ser.offset
    UnsafeSort.UNSAFE.copyMemory(buf, BYTE_ARRAY_BASE_OFFSET, null, addr, 100)
    ser.offset += 100
    (addr, 0L).asInstanceOf[T]
  }

  override def close(): Unit = s.close()
}
