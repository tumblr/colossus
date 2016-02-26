package colossus
package core

import akka.util.{ByteString, ByteStringBuilder}
import java.nio.ByteBuffer

trait DataOutBuffer {

  def available: Long
  def isOverflowed: Boolean

  /**
   * Copy as much as `src` into this buffer as possible.  
   */
  def write(src: DataBuffer)

  /**
   * Attempt to write the entire contents of bytes into this buffer.  If there
   * is not enough available space an exeption is thrown
   */
  def write(bytes: ByteString)

  def write(bytes: Array[Byte])

  /* Get a DataBuffer containing the data written into this DataOutBuffer.  This
   * generally renders this buffer unusable
   *
   * TODO: Unify this maybe with DataBuffer
   */
  def data: DataBuffer

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
class DynamicOutBuffer(baseSize: Int) extends DataOutBuffer {
  
  private val base = ByteBuffer.allocateDirect(baseSize)

  private var dyn: Option[ByteBuffer] = None

  def size = baseSize + dyn.map{_.position}.getOrElse(0)

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

  def copyDestination(bytesNeeded: Long) : ByteBuffer = if (base.remaining >= bytesNeeded) base else {
    while (dynAvailable < bytesNeeded) {
      growDyn()
    }
    dyn.get
  }

  def reset() {
    dyn = None
    base.clear()
  }

  def available = Long.MaxValue //maybe limit this somehow?

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
    copyDestination(bytes.size).put(bytes)
  }

  def data = {
    val d = dyn.getOrElse(base)
    d.flip
    DataBuffer(d)
  }
}
