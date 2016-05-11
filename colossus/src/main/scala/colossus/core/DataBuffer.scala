package colossus
package core

import akka.util.ByteString
import java.nio.ByteBuffer
import java.nio.channels.SocketChannel


/**
 * A low-overhead abstraction over a byte array.  This offers much of the same
 * functionally offered by Akka's ByteString, but without any additional
 * overhead.  While this makes DataBlock a bit less powerful and flexible, it
 * also makes it considerably faster.
 *
 * TODO: This is possibly a contender for using in all places where Array[Byte]
 * is used, but thorough benchmarking is needed.  It is also possible that the
 * performance of ByteString has improved in later versions of Akka, so that
 * should also be tested before expanding on this any more.
 *
 */

case class DataBlock(data: Array[Byte]) {
  
  def size = data.length
  def length = data.length

  def byteString: ByteString = ByteString(data)
  def utf8String = byteString.utf8String

  /**
   * Create a new DataBlock containing the data from `block` appended to the
   * data in this block
   */
  def ++(block: DataBlock): DataBlock = {
    val concat = new Array[Byte](data.length + block.length)
    System.arraycopy(data, 0, concat, 0, data.length)
    System.arraycopy(block.data, 0, concat, data.length, block.length)
    DataBlock(concat)
  }

  def take(bytes: Int): DataBlock = {
    val copy = new Array[Byte](math.min(bytes, size))
    System.arraycopy(data, 0, copy, 0, copy.length)
    DataBlock(copy)
  }

  def drop(bytes: Int): DataBlock = {
    val copy = new Array[Byte](math.max(0, size - bytes))
    System.arraycopy(data, size - copy.length, copy, 0, copy.length)
    DataBlock(copy)
  }


  def apply(index: Int) = data(index)

  override def toString = "DataBlock(" + data.mkString(",") + ")"
  override def equals(that: Any) = that match {
    case DataBlock(dta) => java.util.Arrays.equals(data, dta)
    case _ => false
  }
  override def hashCode = toString.hashCode

}

object DataBlock {

  def apply(str: String): DataBlock = DataBlock(ByteString(str).toArray)
}


/**
 * A DataReader is the result of codec's encode operation.  It can either
 * return a DataBuffer, which contains the entire encoded object at once, or it
 * can return a DataStream, which has a Sink for streaming the encoded object.
 */
sealed trait DataReader

case class DataStream(source: controller.Source[DataBuffer]) extends DataReader

trait Encoder extends DataReader{
  def encode(out: DataOutBuffer)

  def bytes: ByteString = {
    val out = new DynamicOutBuffer(100, false)
    encode(out)
    ByteString(out.data.takeAll)
  }
}

/** A thin wrapper around a NIO ByteBuffer with data to read
 *
 * DataBuffers are the primary way that data is read from and written to a
 * connection.  DataBuffers are mutable and not thread safe.  They can only be
 * read from once and cannot be reset.
 *
 */
case class DataBuffer(data: ByteBuffer) extends Encoder {
  /** Get the next byte, removing it from the buffer
   *
   * WARNING : This method will throw an exception if no data is left.  It is
   * up to you to use hasUnreadData to figure out if you should call this.
   * This is done to avoid unnecessary object allocation with using Option
   *
   * @return the next byte in the buffer
   */
  def next(): Byte = data.get

  private var peeking = false

  def encode(out: DataOutBuffer) {
    out.write(this)
  }


  /** Get some bytes
   * @param n how many bytes you want.
   * @return an filled array of size min(n, remaining)
   */
  def take(n: Int): Array[Byte] = {
    val actualSize = math.min(remaining, n)
    val arr = new Array[Byte](actualSize)
    data.get(arr)
    arr
  }

  /** Returns an array containing all of the unread data in this Databuffer */
  def takeAll: Array[Byte] = take(remaining)

  /** Copy the unread data in this buffer to a new buffer
   *
   * Data will not be shared between the buffers.  The position of this buffer will be completed
   *
   * @return a new DataBuffer containing only the unread data in this buffer
   */
  def takeCopy: DataBuffer = DataBuffer.fromByteString(ByteString(takeAll))

  /** Directly copy data into a target byte array
   * @param buffer the array to copy into
   * @param offset the first index of buffer to start writing to
   * @param length how many bytes to write
   * @throws ArrayOutOfBoundsException if target array is too small or buffer doesn't have sufficient bytes available
   */
  def takeInto(buffer: Array[Byte], offset: Int, length: Int) {
    if (length > remaining) {
      throw new IndexOutOfBoundsException(s"Attempted to take $length bytes from buffer with only $remaining bytes remaining")
    }
    if (offset + length > buffer.length) {
      throw new IndexOutOfBoundsException("Attempted to write too many byte to target array")
    }
    data.get(buffer, offset, length)
  }

  /** Skip over n bytes in the buffer.
   *
   * @param n the number of bytes to skip.
   * @throws IllegalArgumentException if n is larger than the number of remaining bytes
   */
  def skip(n: Int) {
    data.position(data.position() + n)
  }

  def skipAll() {
    skip(remaining)
  }

  /** Write the buffer into a SocketChannel
   *
   * The buffer's taken and remaining values will be updated to reflect how
   * much data was written.  Be aware that buffer's containing large amounts of
   * data will probably not be written in one call
   *
   * @param channel the channel to write to
   * @return how many bytes were written
   */
  def writeTo(channel: SocketChannel) = {
    channel.write(data)
  }

  /** Returns true if this DataBuffer still has unread data, false otherwise */
  def hasNext = data.hasRemaining

  /** Returns true if this DataBuffer still has unread data, false otherwise */
  def hasUnreadData = data.hasRemaining

  def remaining = data.remaining

  /** Returns how many bytes have already been read from this DataBuffer */
  def taken = data.position

  /** Returns the total size of this DataBuffer */
  def size = data.limit

  def peek[T](f: DataBuffer => T): (T, Int) = {
    if (peeking) {
      throw new Exception("Cannot peek into databuffer that is already peeking")
    }
    peeking = true
    data.mark()
    val pos1 = data.position
    val res = f(this)
    val pos2 = data.position
    data.reset()
    peeking = false
    (res, pos2 - pos1)
  }

}

object DataBuffer {
  def apply(buffer: ByteBuffer, bytesRead: Int): DataBuffer = {
    val n = buffer.asReadOnlyBuffer
    n.limit(bytesRead)
    DataBuffer(n)
  }

  def apply(data: Array[Byte]): DataBuffer = DataBuffer(ByteBuffer.wrap(data))

  def apply(data: ByteString): DataBuffer = fromByteString(data)

  def fromByteString(b: ByteString): DataBuffer = DataBuffer(b.asByteBuffer, b.size)
}
