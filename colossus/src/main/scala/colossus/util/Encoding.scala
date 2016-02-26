package colossus
package encoding

import akka.util.{ByteString, ByteStringBuilder}
import java.nio.ByteBuffer
import core.DataBuffer


trait DataOutBuffer {

  def available: Long

  /**
   * Copy as much as `src` into this buffer as possible.  
   */
  def copy(src: DataBuffer)

  /**
   * Attempt to write the entire contents of bytes into this buffer.  If there
   * is not enough available space an exeption is thrown
   */
  def write(bytes: ByteString)

  /* Get a DataBuffer containing the data written into this DataOutBuffer.  This
   * generally renders this buffer unusable
   *
   * TODO: Unify this maybe with DataBuffer
   */
  def data: DataBuffer

}


trait Encodable {
  def encode : DataBuffer
}

case class ByteOutBuffer(underlying: ByteBuffer) extends DataOutBuffer {

  def available = underlying.remaining

  def copy(data: DataBuffer) {
    val src = data.data
    val oldLimit = src.limit()
    val newLimit = if (src.remaining > underlying.remaining) {
      oldLimit - (src.remaining - underlying.remaining)
    } else {
      oldLimit
    }
    src.limit(newLimit)
    underlying.put(src)
    src.limit(oldLimit)
  }

  def write(bytes: ByteString) {
    underlying.put(bytes.asByteBuffer)
  }

  def data = {
    underlying.flip
    DataBuffer(underlying)
  }


}

/*
class DynamicBuffer extends DataOutBuffer {
  
  private val builder = new ByteStringBuilder

  def available = Long.MaxValue //maybe limit this somehow?
  def copy(from: DataBuffer) {
    builder ++= from.takeAll
  }
  def write(bytes: ByteString) {
    builder ++= bytes
  }

  def result = builder.result

  def data = DataBuffer(result)
}
*/
