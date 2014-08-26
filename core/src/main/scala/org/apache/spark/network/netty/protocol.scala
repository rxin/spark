/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.netty

import java.util.{List => JList}

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.handler.codec.{ByteToMessageDecoder, MessageToMessageDecoder, MessageToMessageEncoder}

import org.apache.spark.Logging
import org.apache.spark.network.{NettyByteBufManagedBuffer, ManagedBuffer}


sealed trait ClientRequest {
  def id: Byte
}

final case class BlockFetchRequest(blocks: Seq[String]) extends ClientRequest {
  override def id = 0
}

final case class BlockUploadRequest(blockId: String, data: ManagedBuffer) extends ClientRequest {
  override def id = 1
}


sealed trait ServerResponse {
  def id: Byte
}

final case class BlockFetchSuccess(blockId: String, data: ManagedBuffer) extends ServerResponse {
  override def id = 0
}

final case class BlockFetchFailure(blockId: String, error: String) extends ServerResponse {
  override def id = 1
}


final class ClientRequestEncoder extends MessageToMessageEncoder[ClientRequest] {
  override def encode(ctx: ChannelHandlerContext, in: ClientRequest, out: JList[Object]): Unit = {
    in match {
      case BlockFetchRequest(blocks) =>
        // 8 bytes: frame size
        // 1 byte: BlockFetchRequest vs BlockUploadRequest
        // then for each block id write 1 byte for blockId.length and then blockId itself
        val frameLength = 8 + 1 + blocks.size + blocks.map(_.size).reduce(_ + _)
        val buf = ctx.alloc().buffer(frameLength)

        buf.writeLong(frameLength)
        buf.writeByte(in.id)
        blocks.foreach { blockId =>
          ProtocolUtils.writeBlockId(buf, blockId)
        }

        assert(buf.writableBytes() == 0)
        out.add(buf)

      case BlockUploadRequest(blockId, data) =>
        // 8 bytes: frame size
        // 1 byte: BlockFetchRequest vs BlockUploadRequest
        // 1 byte: blockId.length
        // data itself (length can be derived from: frame size - 1 - blockId.length)
        val headerLength = 8 + 1 + 1 + blockId.length
        val frameLength = headerLength + data.size
        val header = ctx.alloc().buffer(headerLength)

        // Call this before we add header to out so in case of exceptions
        // we don't send anything at all.
        val body = data.toNetty()

        header.writeLong(frameLength)
        header.writeByte(in.id)
        ProtocolUtils.writeBlockId(header, blockId)

        assert(header.writableBytes() == 0)
        out.add(header)
        out.add(body)
    }
  }
}


final class ClientRequestDecoder extends MessageToMessageDecoder[ByteBuf] {
  override protected def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: JList[AnyRef]): Unit =
  {
    val msgTypeId = in.readByte()
    val decoded = msgTypeId match {
      case 0 =>  // BlockFetchRequest
        val numBlocks = in.readByte().toInt
        val blockIds = Seq.fill(numBlocks) { ProtocolUtils.readBlockId(in) }
        BlockFetchRequest(blockIds)

      case 1 =>  // BlockUploadRequest
        val blockId = ProtocolUtils.readBlockId(in)
        in.retain()  // retain the bytebuf so we don't recycle it immediately.
        BlockUploadRequest(blockId, new NettyByteBufManagedBuffer(in))
    }

    assert(decoded.id == msgTypeId)
    out.add(decoded)
  }
}


final class ServerResponseEncoder extends MessageToMessageEncoder[ServerResponse] with Logging {
  override def encode(ctx: ChannelHandlerContext, in: ServerResponse, out: JList[Object]): Unit = {
    in match {
      case BlockFetchSuccess(blockId, data) =>
        // Handle the body first so if we encounter an error getting the body, we can respond
        // with an error instead.
        var body: AnyRef = null
        try {
          body = data.toNetty()
        } catch {
          case e: Exception =>
            // Re-encode this message as BlockFetchFailure.
            logError(s"Error opening block $blockId for client ${ctx.channel.remoteAddress}", e)
            encode(ctx, new BlockFetchFailure(blockId, e.getMessage), out)
            return
        }

        // If we got here, body cannot be null
        val headerLength = 8 + 1 + 1 + blockId.length
        val frameLength = headerLength + data.size
        val header = ctx.alloc().buffer(headerLength)
        header.writeLong(frameLength)
        header.writeByte(in.id)
        ProtocolUtils.writeBlockId(header, blockId)

        assert(header.writableBytes() == 0)
        out.add(header)
        out.add(body)

      case BlockFetchFailure(blockId, error) =>
        val frameLength = 8 + 1 + 1 + blockId.length + error.length
        val buf = ctx.alloc().buffer(frameLength)
        buf.writeLong(frameLength)
        buf.writeByte(in.id)
        ProtocolUtils.writeBlockId(buf, blockId)
        buf.writeBytes(error.getBytes)

        assert(buf.writableBytes() == 0)
        out.add(buf)
    }
  }
}


final class ServerResponseDecoder extends ByteToMessageDecoder {
  override def decode(ctx: ChannelHandlerContext, in: ByteBuf, out: JList[AnyRef]): Unit = {
    val msgId = in.readByte()
    val decoded = in match {
      case 0 =>  // BlockFetchSuccess
        val blockId = ProtocolUtils.readBlockId(in)
        in.retain()
        new BlockFetchSuccess(blockId, new NettyByteBufManagedBuffer(in))

      case 1 =>  // BlockFetchFailure
        val blockId = ProtocolUtils.readBlockId(in)
        val errorBytes = new Array[Byte](in.readableBytes())
        in.readBytes(errorBytes)
        new BlockFetchFailure(blockId, new String(errorBytes))
    }

    assert(decoded.id == msgId)
    out.add(decoded)
  }
}


private object ProtocolUtils {
  def readBlockId(in: ByteBuf): String = {
    val bytes = new Array[Byte](in.readByte().toInt)
    in.readBytes(bytes)
    new String(bytes)
  }

  def writeBlockId(out: ByteBuf, blockId: String): Unit = {
    out.writeByte(blockId.length)
    out.writeBytes(blockId.getBytes)
  }
}
