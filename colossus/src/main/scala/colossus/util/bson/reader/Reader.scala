package colossus.util.bson.reader

import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

trait Reader[T] {

  def buffer: ByteBuffer

  def readCString(): String = readCString(new ArrayBuffer[Byte](16))

  @scala.annotation.tailrec
  private def readCString(array: ArrayBuffer[Byte]): String = {
    val byte = buffer.get()
    if (byte == 0x00)
      new String(array.toArray, "UTF-8")
    else readCString(array += byte)
  }

  def readString(): String = {
    val size = buffer.getInt()
    val array = new Array[Byte](size - 1)
    buffer.get(array)
    buffer.get()
    new String(array)
  }

  def readBytes(num: Int): Array[Byte] = {
    val array = new Array[Byte](num)
    buffer.get(array)
    array
  }

  def read: Option[T]
}
