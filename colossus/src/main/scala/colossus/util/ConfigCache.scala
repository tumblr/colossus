package colossus
package util

import java.util.concurrent.ConcurrentHashMap
import scala.util.Try

abstract class ConfigCache[T] {

  protected def load(path: String): Try[T]

  private val cache = new ConcurrentHashMap[String, Try[T]]

  def get(path: String): Try[T] = {
    Option(cache.get(path)) match {
      case Some(t) => t
      case None => {
        val loaded = load(path)
        cache.putIfAbsent(path, loaded)
        loaded
      }
    }
  }

}
