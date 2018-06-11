package colossus.core

import akka.util.ByteString

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

  def size   = data.length
  def length = data.length

  def byteString: ByteString = ByteString(data)
  def utf8String             = byteString.utf8String

  /**
    * Create a new DataBlock containing the data from `block` appended to the
    * data in this block
    */
  def ++(block: DataBlock): DataBlock = {
    val concat = java.util.Arrays.copyOf(data, data.length + block.length)
    System.arraycopy(block.data, 0, concat, data.length, block.length)
    DataBlock(concat)
  }

  def take(bytes: Int): DataBlock = {
    DataBlock(java.util.Arrays.copyOf(data, Math.min(bytes, size)))
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
    case _              => false
  }
  override def hashCode = toString.hashCode

}

object DataBlock {

  val Empty = DataBlock(Array[Byte]())

  def apply(str: String): DataBlock = DataBlock(ByteString(str).toArray)
}
