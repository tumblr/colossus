package colossus
package core

import akka.util.{ByteString, ByteStringBuilder}
import java.nio.ByteBuffer

/**
 * The DataOutBuffer is a ConnectionHandler's interface for writing data out to
 * the connection.  DataOutBuffers support a "soft" overflow, which means it
 * will dynamically allocate more space as needed, but is based off a
 * fixed-length base size.  The idea is that writing should stop as soon as the
 * overflow is hit, but it is not a requirement.
 */
trait DataOutBuffer {

  def isOverflowed: Boolean

  protected def copyDestination(bytesNeeded: Long): ByteBuffer

  def write(from: ByteBuffer) {
    copyDestination(from.remaining).put(from)
  }

  def write(from: DataBuffer) {
    write(from.data)
  }

  def write(bytes: ByteString) {
    write(bytes.asByteBuffer)
  }

  def write(bytes: Array[Byte]) {
    copyDestination(bytes.length).put(bytes)
  }

  def write(bytes: Array[Byte], offset: Int, length: Int) {
    copyDestination(length).put(bytes, offset, length)
  }

  def write(byte: Byte) {
    copyDestination(1).put(byte)
  }

  def write(char: Char) {
    write(char.toByte)
  }

}

/**
 * A ByteBuffer-backed growable buffer.  A fixed-size direct buffer is used for
 * most of the time, but overflows are written to a eponentially growing
 * non-direct buffer.
 *
 *
 * Be aware, the DataBuffer returned from `data` merely wraps the underlying
 * buffer to avoid copying
 */
class DynamicOutBuffer(baseSize: Int, allocateDirect: Boolean = true) extends DataOutBuffer {

  private val base = if (allocateDirect) {
    ByteBuffer.allocateDirect(baseSize)
  } else {
    ByteBuffer.allocate(baseSize)
  }

  private var dyn: Option[ByteBuffer] = if (allocateDirect) None else Some(base)

  def size = dyn.map{_.position}.getOrElse(base.position)

  def isOverflowed: Boolean = dyn.isDefined

  private def dynAvailable = dyn.map{_.remaining}.getOrElse(0)

  private def growDyn() {
    dyn match {
      case Some(old) => {
        val nd = ByteBuffer.allocate(old.capacity * 2)
        old.flip
        nd.put(old)
        dyn = Some(nd)
      }
      case None => {
        val nd = ByteBuffer.allocate(baseSize * 2)
        base.flip
        nd.put(base)
        dyn = Some(nd)
      }
    }
  }

  protected def copyDestination(bytesNeeded: Long) : ByteBuffer = if (base.remaining >= bytesNeeded) base else {
    while (dynAvailable < bytesNeeded) {
      growDyn()
    }
    dyn.get
  }

  def reset() {
    dyn = None
    base.clear()
  }


  def data = {
    val d = dyn.getOrElse(base)
    d.flip
    DataBuffer(d)
  }
}
